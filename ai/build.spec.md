# Specification: Synod Build and Continuous Integration

## Continuous Integration (CI)

- Setup a GitHub workflow to perform continuous integration testing.
- The workflow must be triggered on pushes to the `main` branch and on any `pull_request` targeting the `main` branch.
- Use Ubuntu latest environment.
- The workflow must explicitly checkout the source code (`actions/checkout`).
- Implement Bazel caching using `actions/cache`.
  - Cache paths: `~/.cache/bazel-disk-cache` and `~/.cache/bazel-repository-cache`.
  - Key: `${{ runner.os }}-bazel-linux-amd64-${{ hashFiles('MODULE.bazel.lock', 'go.sum') }}`.
- Use Bazel for all compilation and testing operations.
- Configure Bazel via `bazelbuild/setup-bazelisk`.
- The workflow must execute two primary steps using `--disk_cache` and `--repository_cache` pointing to the cached directories:
  - `bazel build //...` to ensure all targets compile successfully.
  - `bazel test //...` to ensure all unit and integration tests pass.

## Continuous Delivery / Releases (CD)

- Setup a separate GitHub workflow dedicated to releasing the agent binary.
- The workflow must run automatically on a monthly schedule (e.g., the 1st of every month at midnight UTC) using cron.
- Ensure the workflow can also be triggered manually (`workflow_dispatch`).
- The workflow requires `contents: write` permissions.
- Use a multi-job strategy:
  1. **tag**: Computes and pushes the next semantic version using `mathieudutour/github-tag-action`. Requires `fetch-depth: 0`.
  2. **build**: A matrix job that cross-compiles the agent for multiple platforms.
     - Matrix dimensions: `linux-amd64`, `darwin-amd64`, `darwin-arm64`.
     - Each matrix runner uses an architecture-specific Bazel cache key: `${{ runner.os }}-bazel-${{ matrix.arch }}-${{ hashFiles(...) }}`.
     - Uses Bazel platform flags for cross-compilation.
     - Uploads the resulting binary as a workflow artifact.
  3. **release**: Downloads all artifacts and publishes them to a GitHub Release using `softprops/action-gh-release`.
- Binary naming convention: `synod-agent-${arch}`.
- Enable auto-generated release notes.

## Documentation

- Provide status badges in the project `README.md` file indicating the current status of the Build/Test CI workflow and the Release workflow.
