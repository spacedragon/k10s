# k10s

## Releasing

Release builds are automated by [`.github/workflows/release.yml`](.github/workflows/release.yml) — pushing a tag `v<version>` builds the `k10s-desktop` and `k10s-server` binaries for Linux (x86_64), Windows (x86_64), and macOS (arm64 + x86_64) and publishes them as a GitHub Release with checksums.

To cut a release:

1. Bump `version` under `[workspace.package]` in `Cargo.toml` and merge the change (CI on `main` must stay green).
2. Tag exactly `v<version>` (the workflow fails if the tag does not match the workspace version):

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. Monitor the Release workflow; the release is created automatically with generated release notes.

A manual `workflow_dispatch` run of the same workflow builds all platform artifacts without publishing, which is useful to smoke-test the pipeline before tagging.
