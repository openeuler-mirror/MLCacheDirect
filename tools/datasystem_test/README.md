# datasystem测试代码说明

## 代码说明

### pipeline_h2d
`pipeline_h2d.cpp` 是正常功能/性能联调用例，主要面向手工验证 Set、MGetH2D、普通 Get + cudaMemcpy、批量 H2D、多线程并发这些路径。

### pipeline_async_pin_test
`pipeline_async_pin_test.cpp` 用于验证 datasystem 异步 CUDA Host Memory Pin 场景。工具支持普通一次性测试和
shell 常驻测试；shell 模式只初始化一个 `KVClient`，随后可持续执行单 key、批量和多线程
Create/Set/Get，并通过 H2D、D2H 回拷校验数据正确性，直到输入 `quit` 才销毁 client。

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

### pipeline_async_pin_test

#### 工具用途

该工具用于验证 datasystem 异步 CUDA Host Memory Pin 场景：

1. pin 尚未完成时，Create、Set、Get 不等待 pin，仍能正常执行。
2. Create 返回临时 pageable Buffer 后，CPU memcpy 写入及后续 Set 的数据正确性。
3. Get(ReadOnlyBuffer) 返回临时 pageable Buffer 后，H2D 和 D2H 的数据正确性。
4. pin 完成前后，同一个常驻 KVClient 的单 key、批量和多线程请求均能正常执行。
5. 真正的 MCreate、MSet、批量 Get(ReadOnlyBuffer) 接口功能。

工具需要在带 GPU 和 CUDA Runtime 的设备上运行。第一个位置参数 host 是 client 初始化时连接的
datasystem worker 地址，不是测试工具所在设备的本地 IP。例如 GPU 设备为 141.62.32.111、worker
为 141.62.32.115 时，应在 111 上执行：

    ./pipeline_async_pin_test 141.62.32.115 shell --port=18681 --gpu_id=0

#### 启动格式

    ./pipeline_async_pin_test <host> <set|get|roundtrip|shell> [options]

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| host | 必填 | client 初始化时连接的 worker IP |
| --port | 18481 | worker 服务端口 |
| --count | 1 | 普通模式下每个线程处理的 key 数 |
| --thread | 1 | 普通模式线程数 |
| --value_size | 3670016 | 默认 value 大小，单位为字节 |
| --key_prefix | async_pin | 普通模式自动生成 key 时使用的前缀 |
| --gpu_id | 0 | CUDA 设备编号 |
| --timeout_ms | 60000 | Get 等待超时时间，单位为毫秒 |
| --enable_local_cache | true | 设置 ConnectOptions.enableLocalCache |
| --cleanup_before | true | Create 前删除同名旧对象 |
| --delete_after | false | 普通模式和 parallel 模式完成后删除对象 |

布尔参数支持 true、false、1、0。参数同时支持以下两种格式：

    --port=18681
    --port 18681

当前工具没有暴露 fast_transport_mem_size 参数，因此使用 SDK 默认的 fast transport 内存池大小，
而不是固定使用 2GB。

Get 完成后默认不会删除 KV。普通模式和 parallel 模式只有显式设置 delete_after=true 才会删除；
shell 模式的 get、mget、roundtrip、mroundtrip 均不会自动删除，需要使用 del 命令手工删除。

#### 普通一次性模式

普通模式启动一个共享 KVClient，所有线程执行完后输出统计并退出。

set 对每个 key 执行 Create、CPU memcpy 填充 Buffer、Set：

    ./pipeline_async_pin_test 141.62.32.115 set --port=18681 --count=10 --thread=4 \
        --value_size=3670016 --gpu_id=0

get 对每个 key 执行 Get(ReadOnlyBuffer)、H2D、D2H和数据校验：

    ./pipeline_async_pin_test 141.62.32.115 get --port=18681 --count=10 --thread=4 \
        --value_size=3670016 --gpu_id=0

roundtrip 对每个 key 完整执行 Create、Set、Get和数据校验：

    ./pipeline_async_pin_test 141.62.32.115 roundtrip --port=18681 --count=10 --thread=4 \
        --value_size=3670016 --gpu_id=0

普通模式生成的 key 格式为：

    <key_prefix>_T<thread_id>_<index>

例如 key_prefix=pin、count=2、thread=2 会生成：

    pin_T0_0
    pin_T0_1
    pin_T1_0
    pin_T1_1

普通模式的总 key 数为 count × thread。单独执行 get 时，key、value_size、count 和 thread 必须与
之前的 set 保持一致。

#### shell 常驻模式

推荐使用 shell 模式验证异步 pin：

    ./pipeline_async_pin_test 141.62.32.115 shell --port=18681 --gpu_id=0 \
        --value_size=3670016 --timeout_ms=60000 --enable_local_cache=false \
        --cleanup_before=true

初始化成功后进入：

    [INIT] rc=OK elapsed_us=... client=0x...
    async-pin>

进程只初始化一次 KVClient。每条交互命令执行完成后继续等待下一条命令，输入 quit 或 exit 后才释放
pending Buffer 并销毁 client。

##### 单 key 命令

| 命令 | 说明 |
| --- | --- |
| create key [size] | 调用 Create、通过 CPU memcpy 填充 Buffer，但暂不调用 Set |
| set key | 对同名 key 前面 Create 返回的同一个 pending Buffer 调用 Set |
| get key [size] | Get ReadOnlyBuffer，执行 H2D、D2H和数据校验 |
| roundtrip key [size] | 连续执行 Create、Set、Get和校验 |

Create 和 Set 分离测试：

    create single_before_pin 3670016
    pending
    sleep 30000
    set single_before_pin
    get single_before_pin 3670016

create 成功后，Buffer 保存在工具的 pending 表中。set 成功后才从 pending 表移除；如果 Set 失败，
Buffer 会继续保留，允许再次执行 set 重试。

##### 批量接口命令

| 命令 | 实际调用 | 说明 |
| --- | --- | --- |
| mcreate prefix count [size] | KVClient::MCreate | 一次批量 Create，并保存全部 pending Buffer |
| mset prefix | KVClient::MSet | 发布此前同 prefix 的 MCreate 批次 |
| mget prefix count [size] | 批量 KVClient::Get(ReadOnlyBuffer) | 一次批量 Get，然后逐个 H2D、D2H和校验 |
| mroundtrip prefix count [size] | MCreate、MSet、批量Get | 一次完成完整批量验证 |

批量命令生成的 key 格式为 prefix_index。例如：

    mcreate batch_before_pin 10 3670016

会生成 batch_before_pin_0 到 batch_before_pin_9。

批量 Create 和 Set 分离测试：

    mcreate batch_before_pin 10 3670016
    pending
    sleep 30000
    mset batch_before_pin
    mget batch_before_pin 10 3670016

快速完成一轮批量测试：

    mroundtrip batch_after_pin 10 3670016

##### 多线程并发命令

格式：

    parallel <set|get|roundtrip> <prefix> <总key数> <线程数> [size]

使用8个线程并发处理总计100个 key：

    parallel set parallel_pin 100 8 3670016
    parallel get parallel_pin 100 8 3670016

并发执行完整流程：

    parallel roundtrip parallel_rt 100 8 3670016

parallel 模式生成 prefix_0 到 prefix_count-1。所有线程共享同一个常驻 KVClient，并通过原子计数器
动态领取 key。count 是所有线程合计处理的 key 数，不是每线程数量。

parallel set 和 parallel get 并发调用的是单 key 接口，不是多线程调用 MCreate、MSet或批量Get。

##### 控制命令

| 命令 | 说明 |
| --- | --- |
| pending | 显示已经 Create、但尚未 Set 的 Buffer |
| sleep ms | 保持 client 和 pending Buffer 存活，等待指定毫秒数 |
| status | 打印累计统计、pending Buffer和pending批次 |
| del key | 删除一个已经发布的 key |
| discard key | 释放单 key create 产生的 pending Buffer，不执行 Set |
| help | 显示交互命令帮助 |
| quit 或 exit | 退出 shell 并销毁 client |

sleep 期间 shell 不接收新命令，但 datasystem 内部异步 pin 线程仍会继续运行。不建议对 mcreate 批次中的
单个 key 执行 set 或 discard，应使用 mset prefix 处理完整批次。

#### 数据正确性验证

Set 方向的数据流：

    CPU生成测试数据 -> memcpy -> Create/MCreate Buffer -> Set/MSet

Get 方向的数据流：

    Get ReadOnlyBuffer -> H2D -> GPU -> D2H -> CPU -> 与预期数据逐字节比较

测试数据由 key 和 size 确定性生成，因此同一个 key 在 Set 和 Get 时必须使用相同 size。数据不一致、
Buffer为空、Buffer大小不一致或CUDA拷贝失败都会计为测试失败。

#### 统计字段

| 字段 | 说明 |
| --- | --- |
| create success | 成功Create的key数量 |
| set success | 成功Set的key数量 |
| get success | 成功取得ReadOnlyBuffer的key数量 |
| H2D success | 成功完成的Host-to-Device拷贝次数 |
| D2H success | 成功完成的Device-to-Host拷贝次数 |
| verify success | 数据逐字节校验成功的key数量 |
| failed | 累计失败的操作数量 |
| result | failed为0时是PASS，否则是FAIL |

shell 模式的统计持续累计，目前没有清零命令。执行期间只要出现过失败，退出时返回码就是2；参数或初始化
失败返回1，全部成功返回0。

#### 推荐的异步 pin 测试流程

如果临时将 IsCudaHostMemoryRegistrationDone() 固定为 false，下面所有请求都会持续验证 pin-pending 的
pageable 内存规避路径，不会在 sleep 后自动切换为共享内存路径。

如果要在同一个 client 中比较 pin 完成前后的路径，应恢复该函数的真实状态判断：client 初始化后立即
执行第一组命令，并在 datasystem 日志确认 cudaHostRegister 完成后执行第二组命令。sleep 只负责保持
client 存活并等待，不保证指定时间内 pin 一定完成。

pin 未完成时执行：

    create single_before_pin 3670016
    set single_before_pin
    get single_before_pin 3670016
    mcreate batch_before_pin 10 3670016
    mset batch_before_pin
    mget batch_before_pin 10 3670016
    parallel roundtrip parallel_before_pin 100 8 3670016
    status

在同一个 client 中等待 pin 完成，再验证正常共享内存路径：

    sleep 30000
    roundtrip single_after_pin 3670016
    mroundtrip batch_after_pin 10 3670016
    parallel roundtrip parallel_after_pin 100 8 3670016
    status
    quit

#### 当前限制

1. 批量 MCreate、MSet、批量Get由 shell 主线程发起，不支持多线程并发批量调用。
2. parallel 只支持 set、get、roundtrip，不支持 parallel create 和跨命令保存并发Create Buffer。
3. 只测试 Get(..., Optional<ReadOnlyBuffer>)，不测试 Get(..., Optional<Buffer>)。
4. 使用同步 cudaMemcpy，不测试 cudaMemcpyAsync 和用户传入的 CUDA stream。
5. 不直接调用 MGetH2D/RH2D 接口。
6. 不能直接查询 pin 状态，也不直接显示 Buffer 来自 pageable 内存还是 worker共享内存；需要结合
   datasystem日志判断。
7. 只支持一个常驻 client，不支持在 shell 中切换 worker、重新Init或创建多个client。
8. 不支持批量删除、清空统计和自定义测试数据内容。
9. 未暴露 fast_transport_mem_size，使用 SDK 默认配置。

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
`pipeline_h2d_fault_inject` 必须与 `run_pipeline_h2d_fault_tests.sh` 位于同一目录。
