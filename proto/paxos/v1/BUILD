load("@rules_go//go:def.bzl", "go_library")
load("@rules_go//proto:def.bzl", "go_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")

# gazelle:proto disable_global
# gazelle:proto disable
# gazelle:exclude BUILD

proto_library(
    name = "paxosv1_proto",
    srcs = ["paxos.proto"],
    visibility = ["//visibility:public"],
)

go_proto_library(
    name = "paxosv1_go_proto",
    compilers = [
        "@rules_go//proto:go_proto",
        "@rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/filmil/synod/proto/paxos/v1",
    proto = ":paxosv1_proto",
    visibility = ["//visibility:public"],
)

go_library(
    name = "paxos",
    embed = [":paxosv1_go_proto"],
    importpath = "github.com/filmil/synod/proto/paxos/v1",
    visibility = ["//visibility:public"],
)
