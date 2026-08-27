//! Live Kubernetes storage inventory for the infrastructure projection.

use k8s_openapi::api::{
    core::v1::{PersistentVolume, PersistentVolumeClaim},
    storage::v1::StorageClass,
};
use kube::{Api, ResourceExt, api::ListParams};

use crate::{
    catalog::{CatalogPv, CatalogPvc, CatalogSnapshot, CatalogStorageClass},
    port::{BackendError, QueryResult},
};

use super::KubeAdapter;

impl KubeAdapter {
    pub(super) async fn infrastructure(
        &self,
        context: String,
    ) -> Result<QueryResult, BackendError> {
        if !self.knows_context(&context) {
            return Err(BackendError::NotFound);
        }
        let client = self.cluster_client(&context).await?;
        let claim_api = Api::<PersistentVolumeClaim>::all(client.clone());
        let volume_api = Api::<PersistentVolume>::all(client.clone());
        let class_api = Api::<StorageClass>::all(client);
        let params = ListParams::default();
        let (claims, volumes, classes) = tokio::try_join!(
            claim_api.list(&params),
            volume_api.list(&params),
            class_api.list(&params),
        )
        .map_err(super::read::sanitize_infrastructure_list_error)?;

        let mut claims: Vec<_> = claims.items.into_iter().map(normalize_claim).collect();
        let mut volumes: Vec<_> = volumes.items.into_iter().map(normalize_volume).collect();
        let mut classes: Vec<_> = classes.items.into_iter().map(normalize_class).collect();
        claims.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
        volumes.sort_by(|a, b| a.name.cmp(&b.name));
        classes.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(QueryResult::Infrastructure(CatalogSnapshot::live_storage(
            context,
            self.watches.next_revision(),
            crate::runtime::now_rfc3339(),
            claims,
            volumes,
            classes,
        )))
    }
}

fn normalize_claim(claim: PersistentVolumeClaim) -> CatalogPvc {
    let capacity = claim
        .status
        .as_ref()
        .and_then(|status| status.capacity.as_ref())
        .and_then(|capacity| capacity.get("storage"))
        .or_else(|| {
            claim
                .spec
                .as_ref()
                .and_then(|spec| spec.resources.as_ref())
                .and_then(|resources| resources.requests.as_ref())
                .and_then(|requests| requests.get("storage"))
        })
        .map(|quantity| quantity.0.clone())
        .unwrap_or_default();
    let spec = claim.spec.as_ref();
    CatalogPvc {
        namespace: claim.namespace().unwrap_or_default(),
        name: claim.name_any(),
        status: claim
            .status
            .as_ref()
            .and_then(|status| status.phase.clone())
            .unwrap_or_default(),
        capacity,
        access_modes: spec
            .and_then(|spec| spec.access_modes.clone())
            .unwrap_or_default(),
        storage_class: spec
            .and_then(|spec| spec.storage_class_name.clone())
            .unwrap_or_default(),
        bound_volume: spec
            .and_then(|spec| spec.volume_name.clone())
            .unwrap_or_default(),
        age: created_at(&claim.metadata),
    }
}

fn normalize_volume(volume: PersistentVolume) -> CatalogPv {
    let spec = volume.spec.as_ref();
    let bound_claim = spec
        .and_then(|spec| spec.claim_ref.as_ref())
        .map(|claim| {
            format!(
                "{}/{}",
                claim.namespace.as_deref().unwrap_or_default(),
                claim.name.as_deref().unwrap_or_default()
            )
        })
        .unwrap_or_default();
    CatalogPv {
        name: volume.name_any(),
        status: volume
            .status
            .as_ref()
            .and_then(|status| status.phase.clone())
            .unwrap_or_default(),
        capacity: spec
            .and_then(|spec| spec.capacity.as_ref())
            .and_then(|capacity| capacity.get("storage"))
            .map(|quantity| quantity.0.clone())
            .unwrap_or_default(),
        access_modes: spec
            .and_then(|spec| spec.access_modes.clone())
            .unwrap_or_default(),
        storage_class: spec
            .and_then(|spec| spec.storage_class_name.clone())
            .unwrap_or_default(),
        bound_claim,
        reclaim_policy: spec
            .and_then(|spec| spec.persistent_volume_reclaim_policy.clone())
            .unwrap_or_default(),
        age: created_at(&volume.metadata),
    }
}

fn normalize_class(class: StorageClass) -> CatalogStorageClass {
    CatalogStorageClass {
        name: class.name_any(),
        provisioner: class.provisioner,
        reclaim_policy: class.reclaim_policy.unwrap_or_default(),
        volume_binding_mode: class.volume_binding_mode.unwrap_or_default(),
        age: created_at(&class.metadata),
    }
}

fn created_at(metadata: &kube::core::ObjectMeta) -> String {
    metadata
        .creation_timestamp
        .as_ref()
        .map(|time| time.0.to_string())
        .unwrap_or_default()
}
