# datasystem测试代码说明

## 代码说明

### pipeline_h2d
`pipeline_h2d.cpp` 是正常功能/性能联调用例，主要面向手工验证 Set、MGetH2D、普通 Get + cudaMemcpy、批量 H2D、多线程并发这些路径。

### pipeline_h2d_fault_inject
`pipeline_h2d_fault_test.cpp` 是故障注入自动化用例，不提供通用命令，而是按 scenario 设置 MLCacheDirect 注入点，然后执行一次 MGetH2D，判断结果是否符合预期。

## 编译方式

```bash
mkdir build && cd build
cmake ..
make
```
注：`mlcd_inject_cli` 工具的编译需要依赖当前仓库已通过 `bash build.sh --with-inject` 编译通过，并且将 datasystem 使用的 `libos_transport.so` 替换为带有故障注入点的版本。

## 使用方式

### pipeline_h2d

```
Usage: ./pipeline_h2d <host> [options] [command]
  host    : Server IP address (required, positional)
  command : set | get | mgeth2d | batchget | originget
Options:
  --port=N or --port N            : Server port (default: 18481)
  --count=N or --count N          : Number of keys per thread (default: 10)
  --batch=N or --batch N          : Batch size for batchget (default: 10)
  --value_prefix=X or --value_prefix X  : Base prefix for value (default: 0)
  --keys=k1,k2... or --keys k1,k2     : Comma-separated custom key list
  --value_size=N or --value_size N    : Length of generated value (default: 8388608)
  --thread=N or --thread N            : Number of concurrent threads (default: 1)
  --delete_value=true|false|1|0       : Delete keys after get (default: true)
  --gpu_id=N                         : GPU device ID to use (default: 0)
  --help or -h                        : Show this help message (can be placed anywhere)
Examples:
  ./pipeline_h2d 141.61.91.188 --port=18581 set --keys 123,456 --count 4 --value_prefix a --value_size 8388608 --gpu_id 0 --thread 4
  ./pipeline_h2d 141.61.91.189 --port=18581 get --keys 123,456 --count 4 --value_prefix a --value_size 8388608 --gpu_id 0 --thread 4 --delete_value false
  ./pipeline_h2d 141.61.91.188 --port=18581 set --keys 123,456 --count 4 --value_prefix b --value_size 8388608 --gpu_id 0 --thread 4
  ./pipeline_h2d 141.61.91.189 --port=18581 originget --keys 123,456 --count 4 --value_prefix b --value_size 8388608 --gpu_id 0 --thread 4 --delete_value false
```

#### 限制

+ set 与 get 的参数需保持一致

### pipeline_h2d_fault_inject

`mlcd_inject_cli` 编译后需要将其放到 PATH 中，确保环境中直接可以执行。例如将其复制到 `/usr/bin` 中。如果是远端执行，需确保两端都已放入该二进制，且已配置ssh互信。

`pipeline_h2d_fault_inject` 的典型使用方式为：
```bash
./pipeline_h2d_fault_inject <local_ip> \
    --remote-worker=<remote_ip> \
    --port=<port> \
    --count=<count> \
    --timeout=<timeout> \
    --scenario=<scenario> \
    --inject_delay_ms=<inject_delay_ms>
```
参数说明：
1. local_ip：本地datasystem节点的ip。
2. remote_ip：远端datasystem节点的ip。
3. port：datasystem监听的端口号，本地和远端需使用同一端口号。
4. count：key数量。
5. timeout：执行 `MGetH2D` 操作的超时时间。
6. scenario：测试场景，包括发送、接收等操作时注入延迟或错误，具体见打印。
7. inject_delay_ms: 注入的延时时间。

目前 `pipeline_h2d_fault_inject` 使用的 `libos_transport.so` 是通过dlopen打开，目前已添加大部分常用路径。
在使用时，会显示 `Loaded from: ***` 说明打开的 so 库路径。
可通过环境变量 `MLCACHEDIRECT_LIB_PATH` 设定pipeline_h2d_fault_inject打开的libos_transport.so，确保为datasystem本身使用的so。

集群模式快速测试：`./run_pipeline_h2d_fault_tests.sh <local_ip>  <remote_ip> <port>`。
`pipeline_h2d_fault_inject` 需与 `run_pipeline_h2d_fault_tests.sh` 在同一个目录下。