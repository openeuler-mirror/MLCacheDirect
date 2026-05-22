#!/usr/bin/env bash
set -u
set -o pipefail

# 请在 141.61.91.189 上执行本脚本
# 二进制默认路径：./build/pipeline_h2d
# 如实际路径不同，可这样执行：BIN=/your/path/pipeline_h2d bash ./pipeline_h2d_test_matrix_v2.sh

BIN=${BIN:-./build/pipeline_h2d}
SET_HOST=141.61.91.188
RUN_HOST=141.61.91.189
PORT=18681
GPU=0

SZ_1M=1048576
SZ_2M=2097152
SZ_2M1=2097153
SZ_4M=4194304
SZ_8M=8388608
SZ_10M=10485760
SZ_12M=12582912

CASE_NO=0
PASS_NO=0
FAIL_NO=0

if [[ ! -x "$BIN" ]]; then
  echo "[FATAL] Binary not found or not executable: $BIN"
  echo "[FATAL] 请先编译，或通过 BIN=/path/to/pipeline_h2d 指定二进制路径。"
  exit 1
fi

gen_keys() {
  local prefix="$1"
  local count="$2"
  local out=""
  local i
  for ((i=1; i<=count; i++)); do
    local key
    key=$(printf "%s%02d" "$prefix" "$i")
    if [[ -z "$out" ]]; then
      out="$key"
    else
      out=","$out","$key
    fi
  done
  out=${out#,}
  echo "$out"
}

run_cmd() {
  local expect="$1"
  local desc="$2"
  local cmd="$3"
  CASE_NO=$((CASE_NO + 1))

  echo
  echo "================================================================================"
  echo "[CASE ${CASE_NO}] $desc"
  echo "[EXPECT] $expect"
  echo "[CMD] $cmd"
  echo "================================================================================"

  bash -lc "$cmd"
  local rc=$?

  if [[ "$expect" == FAIL_RC ]]; then
    if [[ $rc -ne 0 ]]; then
      echo "[RESULT] PASS (expected non-zero rc, got rc=$rc)"
      PASS_NO=$((PASS_NO + 1))
    else
      echo "[RESULT] FAIL (expected non-zero rc, got rc=0)"
      FAIL_NO=$((FAIL_NO + 1))
    fi
  elif [[ "$expect" == PASS_RC ]]; then
    if [[ $rc -eq 0 ]]; then
      echo "[RESULT] PASS (rc=0)"
      PASS_NO=$((PASS_NO + 1))
    else
      echo "[RESULT] FAIL (expected rc=0, got rc=$rc)"
      FAIL_NO=$((FAIL_NO + 1))
    fi
  else
    echo "[RESULT] OBSERVE (rc=$rc, 请结合日志核对 EXPECT 描述)"
    PASS_NO=$((PASS_NO + 1))
  fi
}

remote_set() {
  local tag="$1"; local keys="$2"; local count="$3"; local prefix="$4"; local size="$5"; local thread="$6"
  echo "$BIN $SET_HOST --port=$PORT set --keys $keys --count $count --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread"
}

remote_set_nokeys() {
  local count="$1"; local prefix="$2"; local size="$3"; local thread="$4"
  echo "$BIN $SET_HOST --port=$PORT set --count $count --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread"
}

remote_get() {
  local cmdname="$1"; local keys="$2"; local count="$3"; local prefix="$4"; local size="$5"; local thread="$6"; local del="$7"
  echo "$BIN $RUN_HOST --port=$PORT $cmdname --keys $keys --count $count --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread --delete_value $del"
}

remote_get_nokeys() {
  local cmdname="$1"; local count="$2"; local prefix="$3"; local size="$4"; local thread="$5"; local del="$6"
  echo "$BIN $RUN_HOST --port=$PORT $cmdname --count $count --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread --delete_value $del"
}

remote_batchget() {
  local keys="$1"; local count="$2"; local batch="$3"; local prefix="$4"; local size="$5"; local thread="$6"; local del="$7"
  echo "$BIN $RUN_HOST --port=$PORT batchget --keys $keys --count $count --batch $batch --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread --delete_value $del"
}

remote_batchget_nokeys() {
  local count="$1"; local batch="$2"; local prefix="$3"; local size="$4"; local thread="$5"; local del="$6"
  echo "$BIN $RUN_HOST --port=$PORT batchget --count $count --batch $batch --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread --delete_value $del"
}

local_set() {
  local keys="$1"; local count="$2"; local prefix="$3"; local size="$4"; local thread="$5"
  echo "$BIN $RUN_HOST --port=$PORT set --keys $keys --count $count --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread"
}

local_set_nokeys() {
  local count="$1"; local prefix="$2"; local size="$3"; local thread="$4"
  echo "$BIN $RUN_HOST --port=$PORT set --count $count --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread"
}

local_get() {
  local cmdname="$1"; local keys="$2"; local count="$3"; local prefix="$4"; local size="$5"; local thread="$6"; local del="$7"
  echo "$BIN $RUN_HOST --port=$PORT $cmdname --keys $keys --count $count --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread --delete_value $del"
}

local_get_nokeys() {
  local cmdname="$1"; local count="$2"; local prefix="$3"; local size="$4"; local thread="$5"; local del="$6"
  echo "$BIN $RUN_HOST --port=$PORT $cmdname --count $count --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread --delete_value $del"
}

local_batchget() {
  local keys="$1"; local count="$2"; local batch="$3"; local prefix="$4"; local size="$5"; local thread="$6"; local del="$7"
  echo "$BIN $RUN_HOST --port=$PORT batchget --keys $keys --count $count --batch $batch --value_prefix $prefix --value_size $size --gpu_id $GPU --thread $thread --delete_value $del"
}

echo "[INFO] 当前执行机应为: $RUN_HOST"
echo "[INFO] 远端 set 主机:   $SET_HOST"
echo "[INFO] 端口固定:       $PORT"
echo "[INFO] GPU 固定:       $GPU"
echo "[INFO] binary:         $BIN"

# ============================================================
# A. 远端主路径：188 set -> 189 get（重点覆盖，数量更多）
# ============================================================

K_R01="remote_smoke"
run_cmd PASS_RC "远端基础冒烟：188 set 1 个 key，189 get 校验；1MiB，thread=1，delete=false" "$(remote_set R01 "$K_R01" 1 a $SZ_1M 1)"
run_cmd PASS_RC "远端基础冒烟：同一批数据再次 get，验证 delete=false 不删除" "$(remote_get get "$K_R01" 1 a $SZ_1M 1 false)"
run_cmd PASS_RC "远端基础冒烟清理：再次 get 并 delete=true，完成清理" "$(remote_get get "$K_R01" 1 a $SZ_1M 1 true)"

K_R02="remote_del"
run_cmd PASS_RC "远端 delete=true 验证：188 set，189 get 后删除" "$(remote_set R02 "$K_R02" 1 b $SZ_1M 1)"
run_cmd PASS_RC "远端 delete=true：首次 get 成功并删除" "$(remote_get get "$K_R02" 1 b $SZ_1M 1 true)"
run_cmd OBSERVE "远端 delete=true：第二次 get 预期日志出现 Failed keys > 0 或错误信息" "$(remote_get get "$K_R02" 1 b $SZ_1M 1 false) || true"

K_R03="remote_mg_01,remote_mg_02"
run_cmd PASS_RC "远端 mgeth2d 别名路径：count=2，keys=2，标准 2MiB 临界值，不分片，thread=1" "$(remote_set R03 "$K_R03" 2 c $SZ_2M 1)"
run_cmd PASS_RC "远端 mgeth2d：标准 2MiB 临界值，验证别名命令路径" "$(remote_get mgeth2d "$K_R03" 2 c $SZ_2M 1 true)"

K_R04="remote_less_01,remote_less_02"
run_cmd PASS_RC "远端 keys 少于 count：只给 2 个 keys，但 count=4；后两个 key 应自动补 2/3；value_size=2MiB+1，进入分片" "$(remote_set R04 "$K_R04" 4 d $SZ_2M1 1)"
run_cmd PASS_RC "远端 keys<count：189 get 校验自动补 key 行为" "$(remote_get get "$K_R04" 4 d $SZ_2M1 1 true)"

K_R05="$(gen_keys remote_c06_ 6)"
run_cmd PASS_RC "远端 count=6：keys=6，4MiB，thread=2，delete=false" "$(remote_set R05 "$K_R05" 6 e $SZ_4M 2)"
run_cmd PASS_RC "远端 count=6：delete=false，验证 4MiB 分片和双线程并发" "$(remote_get get "$K_R05" 6 e $SZ_4M 2 false)"
run_cmd PASS_RC "远端 count=6：二次 get 清理 delete=true" "$(remote_get get "$K_R05" 6 e $SZ_4M 2 true)"

K_R06="$(gen_keys remote_more_ 6)"
run_cmd PASS_RC "远端 keys 多于 count：提供 6 个 keys，但 count=4；应只使用前 4 个" "$(remote_set R06 "$K_R06" 4 f $SZ_4M 2)"
run_cmd PASS_RC "远端 keys>count：189 get 校验仅前 4 个 key 被使用" "$(remote_get get "$K_R06" 4 f $SZ_4M 2 true)"

run_cmd PASS_RC "远端 count=16：不传 keys，走自动 key 生成；8MiB，thread=4，delete=false" "$(remote_set_nokeys 16 g $SZ_8M 4)"
run_cmd PASS_RC "远端 count=16：不传 keys，8MiB，thread=4，delete=false" "$(remote_get_nokeys get 16 g $SZ_8M 4 false)"
run_cmd PASS_RC "远端 count=16：再次 get 清理 delete=true" "$(remote_get_nokeys get 16 g $SZ_8M 4 true)"

run_cmd PASS_RC "远端 count=32：不传 keys，10MiB，thread=8，压力场景，delete=false" "$(remote_set_nokeys 32 h $SZ_10M 8)"
run_cmd PASS_RC "远端 count=32：10MiB，thread=8，压力场景，delete=false" "$(remote_get_nokeys get 32 h $SZ_10M 8 false)"
run_cmd PASS_RC "远端 count=32：再次 get 清理 delete=true" "$(remote_get_nokeys get 32 h $SZ_10M 8 true)"

run_cmd PASS_RC "远端 count=40：不传 keys，12MiB，thread=8，高压场景，delete=true" "$(remote_set_nokeys 40 i $SZ_12M 8)"
run_cmd PASS_RC "远端 count=40：12MiB，thread=8，高压场景，delete=true" "$(remote_get_nokeys get 40 i $SZ_12M 8 true)"

K_R10="$(gen_keys remote_origin_ 4)"
run_cmd PASS_RC "远端基线对照：originget，count=4，4MiB，thread=2" "$(remote_set R10 "$K_R10" 4 j $SZ_4M 2)"
run_cmd PASS_RC "远端基线对照：originget = 普通 Get + 工具自己 cudaMemcpy" "$(remote_get originget "$K_R10" 4 j $SZ_4M 2 true)"

K_R11="$(gen_keys remote_batch16_ 16)"
run_cmd PASS_RC "远端 batchget：count=16，batch=4，8MiB，thread=4，delete=false" "$(remote_set R11 "$K_R11" 16 k $SZ_8M 4)"
run_cmd PASS_RC "远端 batchget：验证按 4 分批拉取" "$(remote_batchget "$K_R11" 16 4 k $SZ_8M 4 false)"
run_cmd PASS_RC "远端 batchget 后清理：由于 batchget 当前不会 Del，用普通 get delete=true 清理" "$(remote_get get "$K_R11" 16 k $SZ_8M 4 true)"

run_cmd PASS_RC "远端 batchget 高压：count=32，batch=8，10MiB，thread=8，delete=false" "$(remote_set_nokeys 32 l $SZ_10M 8)"
run_cmd PASS_RC "远端 batchget 高压：10MiB，thread=8，batch=8" "$(remote_batchget_nokeys 32 8 l $SZ_10M 8 false)"
run_cmd PASS_RC "远端 batchget 高压后清理：普通 get delete=true" "$(remote_get_nokeys get 32 l $SZ_10M 8 true)"

K_R13="$(gen_keys remote_batch40_ 40)"
run_cmd PASS_RC "远端 batchget 更高压：count=40，batch=10，12MiB，thread=8，delete=true（当前代码里 batchget 不会真的删除）" "$(remote_set R13 "$K_R13" 40 m $SZ_12M 8)"
run_cmd PASS_RC "远端 batchget 更高压：先执行 batchget --delete_value=true" "$(remote_batchget "$K_R13" 40 10 m $SZ_12M 8 true)"
run_cmd PASS_RC "远端行为验证：batchget 之后再次 get 应仍能成功，因为当前 batchget 不执行 Del；这里顺便清理 delete=true" "$(remote_get get "$K_R13" 40 m $SZ_12M 8 true)"

K_R14="r_key-01,r_key_02,rKey03,r.mix.04"
run_cmd PASS_RC "远端 keys 形式覆盖：字母/数字/下划线/横线/点号组合；count=4，2MiB，thread=1" "$(remote_set R14 "$K_R14" 4 n $SZ_2M 1)"
run_cmd PASS_RC "远端 keys 形式覆盖：189 get 校验" "$(remote_get get "$K_R14" 4 n $SZ_2M 1 true)"

# ============================================================
# B. 本机场景：189 set -> 189 get（也覆盖，但少于远端）
# ============================================================

K_L01="local_smoke"
run_cmd PASS_RC "本机基础冒烟：189 set -> 189 get，1MiB，count=1，thread=1，delete=false" "$(local_set "$K_L01" 1 o $SZ_1M 1)"
run_cmd PASS_RC "本机基础冒烟：delete=false 后再次 get 应仍成功" "$(local_get get "$K_L01" 1 o $SZ_1M 1 false)"
run_cmd PASS_RC "本机基础冒烟清理：get delete=true" "$(local_get get "$K_L01" 1 o $SZ_1M 1 true)"

K_L02="$(gen_keys local_c04_ 4)"
run_cmd PASS_RC "本机 count=4：标准 2MiB 临界值，不分片，thread=2" "$(local_set "$K_L02" 4 p $SZ_2M 2)"
run_cmd PASS_RC "本机 count=4：标准 2MiB 临界值，不分片，thread=2" "$(local_get get "$K_L02" 4 p $SZ_2M 2 true)"

K_L03="$(gen_keys local_c06_ 6)"
run_cmd PASS_RC "本机 count=6：2MiB+1，进入分片，thread=2，delete=true" "$(local_set "$K_L03" 6 q $SZ_2M1 2)"
run_cmd PASS_RC "本机 count=6：2MiB+1，进入分片，thread=2" "$(local_get mgeth2d "$K_L03" 6 q $SZ_2M1 2 true)"

run_cmd PASS_RC "本机 count=16：不传 keys，8MiB，thread=4" "$(local_set_nokeys 16 r $SZ_8M 4)"
run_cmd PASS_RC "本机 count=16：不传 keys，8MiB，thread=4" "$(local_get_nokeys get 16 r $SZ_8M 4 true)"

run_cmd PASS_RC "本机 count=40：不传 keys，12MiB，thread=8，高压" "$(local_set_nokeys 40 s $SZ_12M 8)"
run_cmd PASS_RC "本机 count=40：不传 keys，12MiB，thread=8，高压" "$(local_get_nokeys get 40 s $SZ_12M 8 true)"

K_L06="$(gen_keys local_origin_ 6)"
run_cmd PASS_RC "本机基线对照：originget，count=6，4MiB，thread=2" "$(local_set "$K_L06" 6 t $SZ_4M 2)"
run_cmd PASS_RC "本机基线对照：originget" "$(local_get originget "$K_L06" 6 t $SZ_4M 2 true)"

K_L07="$(gen_keys local_batch_ 16)"
run_cmd PASS_RC "本机 batchget：count=16，batch=8，10MiB，thread=8" "$(local_set "$K_L07" 16 u $SZ_10M 8)"
run_cmd PASS_RC "本机 batchget：验证本机批量拉取" "$(local_batchget "$K_L07" 16 8 u $SZ_10M 8 false)"
run_cmd PASS_RC "本机 batchget 后清理：普通 get delete=true" "$(local_get get "$K_L07" 16 u $SZ_10M 8 true)"

# ============================================================
# C. 异常 / 边界 / 代码真实行为覆盖
# ============================================================

K_E01="missing_remote_01"
run_cmd OBSERVE "异常：未先 set 就远端 get；预期日志出现 Failed keys > 0 或错误信息" "$(remote_get get "$K_E01" 1 v $SZ_1M 1 false) || true"

K_E02="$(gen_keys missing_remote_ 6)"
run_cmd OBSERVE "异常：未先 set 就远端 mgeth2d；count=6，8MiB，thread=4；预期日志出现 Failed keys > 0 或错误信息" "$(remote_get mgeth2d "$K_E02" 6 w $SZ_8M 4 false) || true"

K_E03="$(gen_keys missing_origin_ 2)"
run_cmd OBSERVE "异常：未先 set 就远端 originget；预期 rc 可能仍为 0，但日志应体现 get 失败或数据不匹配" "$(remote_get originget "$K_E03" 2 x $SZ_4M 1 false) || true"

K_E04="$(gen_keys bad_batch_ 6)"
run_cmd FAIL_RC "参数异常：batchget 要求 count % batch == 0；这里 count=6,batch=4，预期主程序直接报错退出" "$BIN $RUN_HOST --port=$PORT batchget --keys $K_E04 --count 6 --batch 4 --value_prefix y --value_size $SZ_4M --gpu_id $GPU --thread 1 --delete_value false"

run_cmd FAIL_RC "参数异常：thread=0，预期主程序直接报错退出" "$BIN $RUN_HOST --port=$PORT get --keys bad_thread --count 1 --value_prefix z --value_size $SZ_1M --gpu_id $GPU --thread 0 --delete_value false"

run_cmd PASS_RC "边界行为：count=0 但提供 3 个 keys；代码会回退为按 keys.size()=3 生成并处理数据" "$BIN $SET_HOST --port=$PORT set --keys zero_k1,zero_k2,zero_k3 --count 0 --value_prefix A --value_size $SZ_1M --gpu_id $GPU --thread 1"
run_cmd PASS_RC "边界行为：count=0 且提供 3 个 keys；189 get 应按 3 个 keys 校验并清理" "$BIN $RUN_HOST --port=$PORT get --keys zero_k1,zero_k2,zero_k3 --count 0 --value_prefix A --value_size $SZ_1M --gpu_id $GPU --thread 1 --delete_value true"

run_cmd PASS_RC "边界行为：count=0 且不提供 keys；代码会默认 actual_count=10，在 188 set 10 个自动 key" "$BIN $SET_HOST --port=$PORT set --count 0 --value_prefix B --value_size $SZ_1M --gpu_id $GPU --thread 1"
run_cmd PASS_RC "边界行为：count=0 且不提供 keys；189 get 应按默认 10 个自动 key 校验并清理" "$BIN $RUN_HOST --port=$PORT get --count 0 --value_prefix B --value_size $SZ_1M --gpu_id $GPU --thread 1 --delete_value true"

K_E08="delete_parse"
run_cmd PASS_RC "边界行为：delete_value=abc；当前代码 ParseBool 会把非法值当 true，这里先 set" "$BIN $SET_HOST --port=$PORT set --keys $K_E08 --count 1 --value_prefix C --value_size $SZ_1M --gpu_id $GPU --thread 1"
run_cmd PASS_RC "边界行为：delete_value=abc；当前代码会按 true 处理，因此这次 get 后应删除" "$BIN $RUN_HOST --port=$PORT get --keys $K_E08 --count 1 --value_prefix C --value_size $SZ_1M --gpu_id $GPU --thread 1 --delete_value abc"
run_cmd OBSERVE "边界行为验证：再次 get 预期日志出现 Failed keys > 0 或错误信息，因为上一条 delete_value=abc 实际等价 true" "$BIN $RUN_HOST --port=$PORT get --keys $K_E08 --count 1 --value_prefix C --value_size $SZ_1M --gpu_id $GPU --thread 1 --delete_value false || true"

run_cmd PASS_RC "边界行为：未知命令 unknown；主进程通常 rc=0，但 worker 日志会打印 Unknown command" "$BIN $RUN_HOST --port=$PORT unknown --keys u1 --count 1 --value_prefix D --value_size $SZ_1M --gpu_id $GPU --thread 1"

K_E10="remote_empty_01,,remote_empty_03"
run_cmd PASS_RC "边界行为：keys 中包含空项；空项位置应回退为自动 key，count=3，先在 188 set" "$BIN $SET_HOST --port=$PORT set --keys $K_E10 --count 3 --value_prefix E --value_size $SZ_2M1 --gpu_id $GPU --thread 1"
run_cmd PASS_RC "边界行为：keys 中包含空项；189 get 校验并清理" "$BIN $RUN_HOST --port=$PORT get --keys $K_E10 --count 3 --value_prefix E --value_size $SZ_2M1 --gpu_id $GPU --thread 1 --delete_value true"

echo
echo "================================================================================"
echo "[SUMMARY] total=$CASE_NO pass=$PASS_NO fail=$FAIL_NO"
echo "[NOTE] 标记为 OBSERVE 的用例，请结合日志人工确认是否满足 EXPECT 描述。"
echo "================================================================================"

if [[ $FAIL_NO -ne 0 ]]; then
  exit 1
fi
