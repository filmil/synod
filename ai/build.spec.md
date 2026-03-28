# Specification: Synod Build and Continuous Integration

## Continuous Integration (CI)

* Setup a GitHub workflow to perform continuous integration testing.
* The workflow must be triggered on pushes to the `main` branch and on any `pull_request` targeting the `main` branch.
* Use Ubuntu latest environment.
* The workflow must explicitly checkout the source code (`actions/checkout`).
* Use Bazel for all compilation and testing operations.
* Configure Bazel via `bazelbuild/setup-bazelisk`.
* The workflow must execute two primary steps:
  * `bazel build //...` to ensure all targets compile successfully.
  * `bazel test //...` to ensure all unit and integration tests pass.

## Continuous Delivery / Releases (CD)

* Setup a separate GitHub workflow dedicated to releasing the agent binary.
* The workflow must run automatically on a monthly schedule (e.g., the 1st of every month at midnight UTC) using cron.
* Ensure the workflow can also be triggered manually (`workflow_dispatch`).
* The workflow requires `contents: write` permissions to be able to tag and publish releases.
* When checking out the repository, it must fetch all history (`fetch-depth: 0`) so that previous tags can be discovered and used for semantic version calculation.
* The workflow must calculate the next semantic release version automatically based on the repository tags.
  * Use the `mathieudutour/github-tag-action` action to perform this calculation.
  * The default version bump should be a `minor` increment.
  * The action should push the new version tag to the repository.
* The agent binary must be cross-compiled for multiple architectures using Go's native cross-compilation capabilities via Bazel platforms flags.
  * Compile for Linux AMD64: `--platforms=@rules_go//go/toolchain:linux_amd64`
  * Compile for macOS Intel (AMD64): `--platforms=@rules_go//go/toolchain:darwin_amd64`
  * Compile for macOS Apple Silicon (ARM64): `--platforms=@rules_go//go/toolchain:darwin_arm64`
  * Ensure all binaries are built with optimizations (`-c opt`).
* After compilation, rename the output binaries appropriately to include their target OS and architecture (e.g., `synod-agent-linux-amd64`, `synod-agent-darwin-amd64`, `synod-agent-darwin-arm64`) and ensure they are marked executable.
* Create a formal GitHub Release using the computed semantic tag.
  * Use the `softprops/action-gh-release` action.
  * Include all three cross-compiled binary artifacts in the release payload.
  * Enable auto-generated release notes.

## Documentation

* Provide status badges in the project `README.md` file indicating the current status of the Build/Test CI workflow and the Release workflow.