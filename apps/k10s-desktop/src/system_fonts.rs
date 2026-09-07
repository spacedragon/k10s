use std::sync::Arc;

use eframe::egui::{Context, FontData, FontDefinitions, FontFamily};
use fontdb::{Database, Family, Query};

const CJK_FONT_CANDIDATES: &[&[&str]] = &[
    &[
        "PingFang SC",
        "Microsoft YaHei",
        "Noto Sans CJK SC",
        "WenQuanYi Zen Hei",
    ],
    &["Hiragino Sans", "Yu Gothic", "Meiryo", "Noto Sans CJK JP"],
    &["Apple SD Gothic Neo", "Malgun Gothic", "Noto Sans CJK KR"],
];

pub(crate) fn install_cjk_fallbacks(context: &Context) -> Vec<String> {
    let mut database = Database::new();
    database.load_system_fonts();

    let mut definitions = FontDefinitions::default();
    let mut loaded_ids = Vec::new();
    let mut loaded_families = Vec::new();

    for candidates in CJK_FONT_CANDIDATES {
        let families = candidates
            .iter()
            .copied()
            .map(Family::Name)
            .collect::<Vec<_>>();
        let Some(id) = database.query(&Query {
            families: &families,
            ..Query::default()
        }) else {
            continue;
        };
        if loaded_ids.contains(&id) {
            continue;
        }

        let Some((font_data, face_index)) =
            database.with_face_data(id, |bytes, face_index| (bytes.to_vec(), face_index))
        else {
            continue;
        };
        let family_name = database
            .face(id)
            .and_then(|face| face.families.first())
            .map_or_else(|| "unknown".to_owned(), |family| family.0.clone());
        let key = format!("system-cjk-{}", loaded_ids.len());
        let mut font_data = FontData::from_owned(font_data);
        font_data.index = face_index;
        definitions
            .font_data
            .insert(key.clone(), Arc::new(font_data));

        for family in [FontFamily::Proportional, FontFamily::Monospace] {
            definitions
                .families
                .entry(family)
                .or_default()
                .push(key.clone());
        }
        loaded_ids.push(id);
        loaded_families.push(family_name);
    }

    if !loaded_families.is_empty() {
        context.set_fonts(definitions);
    }
    loaded_families
}
