# 编译 

```
mkdir build
cd build
cmake ..
make
```

# 使用

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

# 限制

+ set 与 get 的参数需保持一致