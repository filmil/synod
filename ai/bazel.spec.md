## Initialize a new repository for use with bazel

- Start recording how many tokens are used.

- Pin bazel version to 9.0.1
- Configure the repository for use with bazel and rules_go.
- Configure the repository for use with gazelle, add build target for gazelle.
- Configure the repository for use with protoc as a prebuilt bazel toolchain.
- Configure the repository for use with grpc rules for bazel.
- Configure the repository with github workflows for build and test.
- Configure .bazelrc to load user.bazelrc if it exists.
- Use `gh` to manipulate github.

- When done, print out the stats on token use for the entire operation.
