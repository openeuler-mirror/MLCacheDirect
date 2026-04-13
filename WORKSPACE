workspace(name = "os_transport")

load("@bazel_tools//tools/build_defs/repo:local.bzl", "new_local_repository")

# 本地 CUDA 仓库
new_local_repository(
    name = "local_cuda",
    path = "/usr/local/cuda",
    build_file = "//third_party:BUILD.cuda",
)

# 本地 URMA 仓库
new_local_repository(
    name = "local_urma",
    path = "/usr",
    build_file = "//third_party:BUILD.urma",
)