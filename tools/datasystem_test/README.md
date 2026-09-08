# datasystem测试代码说明

## 代码说明

### pipeline_h2d
`pipeline_h2d.cpp` 是合并后的联调用例，涵盖：
- h2d 风格命令（`set`、`mgeth2d`、`batchget`、`originget`）：确定性数据（value_prefix / keys / value_size），单 host，
  手工验证 Set、MGetH2D、普通 Get + cudaMemcpy、批量 H2D、多线程并发。
- batch 风格命令（`rh2d`、`get`、`kps`）：随机/缓存数据，remoteip 预置 + localip 拉取，
  `get` 统一为批量 Get + H2D 语义，`kps` 为持续 set-get-del 压测。

### pipeline_async_pin_test
`pipeline_async_pin_test.cpp` 用于验证 datasystem 异步 CUDA Host Memory Pin 场景。工具支持普通一次性测试和
shell 常驻测试；shell 模式只初始化一个 `KVClient`，随后可持续执行单 key、批量、多线程和QPS限速的
卸载（Create、D2H、Set）及加载（Get、H2D），直到输入 `quit` 才销毁 client。

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

支持两种调用风格，第一个位置参数决定风格：

```
# h2d 风格（<host> 开头，兼容原 pipeline_h2d 用法）
Usage: ./pipeline_h2d <host> [options] <command>
  host    : Server IP address (required, positional)
  command : set | mgeth2d | batchget | originget

# batch 风格（已知命令开头）
Usage: ./pipeline_h2d <command> [options]
  command : rh2d | get | kps
```

命令说明：

| 命令 | 语义 |
| --- | --- |
| set | h2d 风格：对确定性生成的 key 执行 Set |
| mgeth2d | h2d 风格：单次 MGetH2D（支持 --delete_value） |
| batchget | h2d 风格：批量 MGetH2D（不 Del，要求 count % batch == 0） |
| originget | h2d 风格：普通 Get + cudaMemcpy（支持 --delete_value） |
| rh2d | batch 风格：remoteip 预置数据，localip 批量 MGetH2D |
| get | batch 风格：remoteip 预置数据，localip 批量 Get + H2D |
| kps | batch 风格：持续 set-get-del 压测 |

选项：

```
  --port=N or --port N            : Server port (default: 18481)
  --count=N or --count N          : Number of keys per thread (default: 10)
  --batch=N or --batch N          : Batch size for batchget/batch 命令 (default: 10)
  --value_prefix=X or --value_prefix X  : Base prefix for value (default: 0)
  --keys=k1,k2... or --keys k1,k2     : Comma-separated custom key list
  --value_size=N or --value_size N    : Length of generated value (default: 8388608)
  --thread=N or --thread N            : Number of concurrent threads (default: 1)
  --delete_value=true|false|1|0       : Delete keys after mgeth2d/originget (default: true; batchget 忽略)
  --pin=true|false|1|0                : Register CUDA host-memory funcs before KVClient Init (default: true)
  --gpu_id=N                         : GPU device ID to use (default: 0)
  --remoteip=IP                       : Worker IP used for Set/Del (batch 命令必填)
  --localip=IP                        : Worker IP used for client init (default: same as remoteip)
  --valuesize=CFG                     : Value size config (batch 命令，size1:num1,size2:num2)
  --verify=Y/N                        : Verify data (default: Y)
  --use_user_stream=Y/N               : Use new MGetH2D interface (default: N)
  --kps=N                             : Target KPS for kps mode
  --duration=N                        : Duration in seconds for kps mode
  --help or -h                        : Show this help message
```

Examples:

```
  # h2d 风格：188 set，189 mgeth2d（单次 MGetH2D）
  ./pipeline_h2d 141.61.91.188 --port=18581 set --keys 123,456 --count 4 --value_prefix a --value_size 8388608 --gpu_id 0 --thread 4
  ./pipeline_h2d 141.61.91.189 --port=18581 mgeth2d --keys 123,456 --count 4 --value_prefix a --value_size 8388608 --gpu_id 0 --thread 4 --delete_value false
  # h2d 风格：originget（普通 Get + cudaMemcpy）
  ./pipeline_h2d 141.61.91.189 --port=18581 originget --keys 123,456 --count 4 --value_prefix b --value_size 8388608 --gpu_id 0 --thread 4 --delete_value false
  # batch 风格：批量 MGetH2D
  ./pipeline_h2d rh2d --count=100 --batch=10 --thread=4 --valuesize=1048576 --remoteip=192.168.1.100 --localip=192.168.1.101
```

#### 限制

+ h2d 风格的 set 与 get（mgeth2d/origininget/batchget）参数需保持一致。

### pipeline_async_pin_test

#### 工具用途

该工具用于验证 datasystem 异步 CUDA Host Memory Pin 场景：

1. pin 尚未完成时，Create、Set、Get 不等待 pin，仍能正常执行。
2. Create 返回 worker 共享内存 Buffer 后，通过 DsCudaMemcpyAsync D2H 写入并等待完成，再执行 Set。
3. Get(ReadOnlyBuffer) 返回 worker 共享内存 Buffer 后，通过 DsCudaMemcpyAsync H2D 并等待完成。
4. pin 完成前后，同一个常驻 KVClient 的单 key、批量和多线程请求均能正常执行。
5. 真正的 MCreate、MSet、批量 Get(ReadOnlyBuffer) 接口，以及批量D2H/H2D功能。
6. 可选正确性校验，以及 Create、D2H、Set、Get、H2D 的并发延迟分位数统计。

工具需要在带 GPU 和 CUDA Runtime 的设备上运行。支持以下三种 Client 初始化方式：

1. Coordinator 服务发现：配置 `--coordinator_address`，通过 Coordinator 发现 Worker。
2. ETCD 服务发现：配置 `--etcd_address`，通过 ETCD 发现 Worker。
3. 直连模式：通过第一个位置参数指定一个 Worker，保留用于单 Worker 回归测试。

Coordinator 和 ETCD 地址只能配置一个；多 Worker 异步 Pin 测试应使用实际部署对应的服务发现方式。

Coordinator 服务发现示例：

    ./pipeline_async_pin_test shell --coordinator_address=141.62.32.115:12159 \
        --cluster_name=zhaopai --host_id_env_name=POD_IP --gpu_id=0

ETCD 服务发现示例：

    ./pipeline_async_pin_test shell --etcd_address=141.62.32.115:12159 \
        --cluster_name=zhaopai --host_id_env_name=POD_IP --gpu_id=0

服务发现模式自动采用以下配置：

    enableLocalCache = false
    enableCrossNodeConnection = true
    dataPlacementPolicy = PREFERRED_META_OWNER
    ServiceDiscovery affinityPolicy = PREFERRED_SAME_NODE

`--host_id_env_name` 填的是环境变量名称，不是 host ID 本身。需要按本机 Worker 启动时使用的
`host_id_env_name` 设置同名环境变量，例如：

    export POD_IP=141.62.32.111

然后传入 `--host_id_env_name=POD_IP`。环境变量的值必须与对应服务发现后端的 membership 中本机 Worker
的 `hostId` 一致。同一台机器上的多个 Worker 应注册相同的 `hostId`，服务发现才能把它们都识别为本机
Worker。该参数不填写时仍可发现 Worker，但无法基于 host ID 区分本机与远端 Worker。

直连模式下，第一个位置参数 host 是 client 初始化时连接的 Worker 地址，不是测试工具所在设备的本地
IP。例如 GPU 设备为 141.62.32.111、Worker 为 141.62.32.115 时，应在 111 上执行：

    ./pipeline_async_pin_test 141.62.32.115 shell --port=18681 --gpu_id=0

#### 启动格式

    ./pipeline_async_pin_test <create_set|get|roundtrip|shell> --coordinator_address=<addr> [--cluster_name=<name>] [options]
    ./pipeline_async_pin_test <create_set|get|roundtrip|shell> --etcd_address=<addr> --cluster_name=<name> [options]
    ./pipeline_async_pin_test <host> <create_set|get|roundtrip|shell> [options]

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| host | 直连模式必填 | client 初始化时连接的 Worker IP；服务发现模式不填写 |
| --port | 18481 | worker 服务端口 |
| --coordinator_address | 无 | Coordinator服务发现地址；多个地址使用逗号分隔，不能与ETCD地址同时配置 |
| --etcd_address | 无 | ETCD服务发现地址；多个地址使用逗号分隔，不能与Coordinator地址同时配置 |
| --cluster_name | 空 | Coordinator模式可选；ETCD模式必填 |
| --host_id_env_name | 无 | 保存本机host ID的环境变量名；需要识别同机多个Worker时建议填写 |
| --count | 1 | 普通模式下每个线程处理的 key 数 |
| --thread | 1 | 普通模式线程数 |
| --value_size | 3670016 | 默认 value 大小，单位为字节 |
| --key_prefix | async_pin | 普通模式自动生成 key 时使用的前缀 |
| --gpu_id | 0 | CUDA 设备编号 |
| --timeout_ms | 60000 | Get 等待超时时间，单位为毫秒 |
| --enable_local_cache | true | 直连模式的配置；服务发现模式固定为false |
| --cleanup_before | true | Create 前删除同名旧对象 |
| --delete_after | false | create_set/Get/roundtrip完成后删除已发布对象 |
| --verify | false | 是否生成确定性数据并执行额外正确性校验；性能测试保持false |

布尔参数支持 true、false、1、0。参数同时支持以下两种格式：

    --port=18681
    --port 18681

当前工具没有暴露 fast_transport_mem_size 参数，因此使用 SDK 默认的 fast transport 内存池大小，
而不是固定使用 2GB。

Get 完成后默认不会删除 KV。普通、shell、parallel和qps模式只有显式设置 delete_after=true 才会删除。

#### 普通一次性模式

普通模式启动一个共享 KVClient，所有线程执行完后输出统计并退出。

create_set 对每个 key 完整执行 Create、D2H和Set，各阶段分别统计：

    ./pipeline_async_pin_test 141.62.32.115 create_set --port=18681 --count=10 --thread=4 \
        --value_size=3670016 --gpu_id=0

get 在正式计时前创建并发布对象，正式阶段执行 Get(ReadOnlyBuffer)和H2D：

    ./pipeline_async_pin_test 141.62.32.115 get --port=18681 --count=10 --thread=4 \
        --value_size=3670016 --gpu_id=0

roundtrip 对每个 key 完整执行 Create、D2H、Set、Get和H2D：

    ./pipeline_async_pin_test 141.62.32.115 roundtrip --port=18681 --count=10 --thread=4 \
        --value_size=3670016 --gpu_id=0

普通模式生成的 key 格式为：

    <key_prefix>_T<thread_id>_<index>

例如 key_prefix=pin、count=2、thread=2 会生成：

    pin_T0_0
    pin_T0_1
    pin_T1_0
    pin_T1_1

普通模式的总 key 数为 count × thread。get模式会在正式计时前自动准备已发布对象。

#### shell 常驻模式

推荐使用服务发现加 shell 常驻模式验证多 Worker 异步 Pin：

    ./pipeline_async_pin_test shell --coordinator_address=141.62.32.115:12159 \
        --cluster_name=zhaopai --host_id_env_name=POD_IP --gpu_id=0 --value_size=3670016 \
        --timeout_ms=60000 --cleanup_before=true

如果集群使用 ETCD：

    ./pipeline_async_pin_test shell --etcd_address=141.62.32.115:12159 \
        --cluster_name=zhaopai --host_id_env_name=POD_IP --gpu_id=0 --value_size=3670016 \
        --timeout_ms=60000 --cleanup_before=true

如需验证原有单 Worker 直连方式：

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
| create_set key [size] | 连续执行Create、DsCudaMemcpyAsync D2H、等待D2H完成和Set；命令返回前释放Buffer |
| get key [size] | Get ReadOnlyBuffer，通过 DsCudaMemcpyAsync 执行 H2D 并等待完成 |
| roundtrip key [size] | 连续执行 Create、D2H、Set、Get和H2D |

执行一次完整卸载，再加载验证：

    create_set single_before_pin 3670016
    get single_before_pin 3670016

create_set 无论成功或失败都不会在命令返回后保留 pending Buffer。

##### 批量接口命令

| 命令 | 实际调用 | 说明 |
| --- | --- | --- |
| mcreate_set prefix count [size] | KVClient::MCreate、批量DsCudaMemcpyAsync D2H、KVClient::MSet | 完成一次批量卸载，命令返回前释放全部Buffer |
| mget prefix count [size] | 批量 KVClient::Get、DsCudaMemcpyAsync H2D | 一次批量 Get，将所有 Buffer H2D 后统一等待完成 |
| mroundtrip prefix count [size] | MCreate、D2H、MSet、批量Get、H2D | 一次完成完整批量卸载和加载 |

批量命令生成的 key 格式为 prefix_index。例如：

    mcreate_set batch_before_pin 10 3670016

会生成 batch_before_pin_0 到 batch_before_pin_9。

批量卸载和加载测试：

    mcreate_set batch_before_pin 10 3670016
    mget batch_before_pin 10 3670016

快速完成一轮批量测试：

    mroundtrip batch_after_pin 10 3670016

##### 多线程并发命令

格式：

    parallel <op> <prefix> <request_count> <threads> [size] [batch_size]

op 支持 create_set、mcreate_set、get、mget、roundtrip和mroundtrip。批量操作中的 request_count
表示批请求数，batch_size 表示每次批请求中的对象数；省略 batch_size 时使用启动行参数 --count。

使用8个线程并发处理总计100个 key：

    parallel create_set parallel_pin 100 8 3670016
    parallel get parallel_pin 100 8 3670016

并发执行完整流程：

    parallel roundtrip parallel_rt 100 8 3670016

使用8个线程并发执行100次批量请求，每次请求包含10个对象：

    parallel mroundtrip parallel_batch 100 8 3670016 10

parallel 模式的单对象 key 为 prefix_requestIndex；批量请求中的 key 为
prefix_requestIndex_itemIndex。所有线程共享同一个常驻 KVClient，并通过原子计数器动态领取请求。
request_count 是所有线程合计处理的请求数，不是每线程数量。

parallel create_set/mcreate_set 在每个计时请求内完成Create/MCreate、D2H和Set/MSet，不保留pending Buffer；
parallel get/mget 会在正式计时前自动创建并发布测试对象。预准备耗时不进入并发阶段统计。

##### QPS限速命令

格式：

    qps <op> <prefix> <qps> <time_seconds> <threads> [size] [batch_size]

例如用8个工作线程，以总计100 QPS持续执行60秒完整单对象卸载和加载：

    qps roundtrip qps_rt 100 60 8 3670016

以总计50 QPS持续执行30秒批量请求，每批包含10个对象：

    qps mroundtrip qps_batch 50 30 8 3670016 10

生产线程按固定时间间隔产生请求，工作线程不足时请求进入队列而不会丢弃。time_seconds 到期后停止
产生新请求，并等待队列排空。报告输出目标和实际QPS、发压和排空耗时，以及 Create、D2H、Set、Get、
H2D、排队和请求总耗时的平均值、P95、P99、P99.99和最大值。

默认 --verify=false，不执行额外校验。使用 --verify=true 时，工具会按 key 生成确定性数据，D2H前预装
到 GPU，H2D后额外回拷到普通 Host 内存进行比较；准备、校验回拷和比较不计入各业务阶段延迟，
但会计入 request_total，并降低整场测试的实际吞吐量。

##### 控制命令

| 命令 | 说明 |
| --- | --- |
| pending | 显示已经 Create、但尚未 Set 的 Buffer |
| sleep ms | 保持 client 和 pending Buffer 存活，等待指定毫秒数 |
| status | 打印累计统计、pending Buffer和pending批次 |
| del key | 删除一个已经发布的 key |
| discard key | 兼容性诊断命令；create_set不在命令间保留pending Buffer，通常返回erased=0 |
| help | 显示交互命令帮助 |
| quit 或 exit | 退出 shell 并销毁 client |

sleep 期间 shell 不接收新命令，但 datasystem 内部异步 pin 线程仍会继续运行。

#### 数据正确性验证

卸载方向的数据流：

    GPU源Buffer -> Create/MCreate -> DsCudaMemcpyAsync D2H -> 同步 -> Set/MSet

加载方向的数据流：

    Get ReadOnlyBuffer -> DsCudaMemcpyAsync H2D -> 同步 -> GPU目标Buffer

verify=false时GPU源Buffer在测试前统一初始化为固定内容，不进行额外比较。verify=true时测试数据由key和
size确定性生成，H2D完成后额外D2H到普通Host校验区逐字节比较。数据不一致、Buffer为空、Buffer大小
不一致或CUDA拷贝失败都会计为测试失败。

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

client 初始化后立即执行第一组命令，可验证分片 pin 进行期间直接访问 worker 共享内存的功能。此时
`DsCudaMemcpyAsync` 会按照共享内存的计划 pin 分片边界拆分拷贝；已经完成注册的分片能够享受 pinned
memory 的传输收益，尚未注册的分片仍可正常拷贝。

在 datasystem 日志确认 cudaHostRegister 完成后执行第二组命令，可在同一个 client 中验证 pin 完成后的
路径。sleep 只负责保持 client 存活并等待，不保证指定时间内 pin 一定完成。

pin 未完成时执行：

    create_set single_before_pin 3670016
    get single_before_pin 3670016
    mcreate_set batch_before_pin 10 3670016
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

1. 只测试 Get(..., Optional<ReadOnlyBuffer>)，不测试 Get(..., Optional<Buffer>)。
2. H2D和D2H通过同一个用户CUDA stream调用KVClient::DsCudaMemcpyAsync，并在进入依赖它的下一阶段前
   同步该stream；
   尚未提供原生cudaMemcpy对照模式。
3. 不直接调用 MGetH2D/RH2D 接口。
4. 不能直接查询 pin 状态；需要结合 datasystem 日志判断各分片是否完成注册。
5. 只支持一个常驻 client，不支持在 shell 中切换 worker、重新Init或创建多个client。
6. 不支持批量删除、清空统计和自定义测试数据内容。
7. 未暴露 fast_transport_mem_size，使用 SDK 默认配置。

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
