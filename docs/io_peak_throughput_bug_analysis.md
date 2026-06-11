# FalconStats IO Peak Throughput — Bug 分析报告

## 总览

| 等级 | 数量 | 说明 |
|------|------|------|
| 🟡 Minor | 2 | 防御性不足 / 边界处理不完善 |
| 🟢 Trivial | 1 | 日志级别问题 |

---

## 架构确认 ✅

该特性的跨进程数据流架构是**正确的**：

### FUSE 进程（`fuse_main.cpp:488-491`）
```cpp
falcon::brpc_io::RemoteIOServer &server = falcon::brpc_io::RemoteIOServer::GetInstance();
server.endPoint = FLAGS_rpc_endpoint;
std::thread brpcServerThread(&falcon::brpc_io::RemoteIOServer::Run, &server);
```

FUSE 进程启动 brpc server 监听 `0.0.0.0:56039`，`RemoteIOServiceImpl::ReportIORecords` 是接收 Store RPC 的 handler。

### 完整数据流

```
Store 进程 (非FUSE):
  FalconStore I/O → IOStatDuration → FalconStats::startIO/finishIO
  reportingThreadFunc (每1s):
    → getRecordsForReport(pid)       // 获取本周期新增的已完成记录 + inflight
    → client.ReportIORecords(...)    // brpc RPC → FUSE:56039
    → cleanupReportedRecords(...)    // 删除已上报记录（增量上报）

FUSE 进程:
  brpc server → RemoteIOServiceImpl::ReportIORecords()
    → IORecordAggregator::receiveIORecords(nodeId, pid, records)

  selfReportThread (每1s):
    → getRecordsForReport() → receiveIORecords(-1, pid, records)

  peakCalcThread (每N秒):
    → aggregateAndPrintPeak(READ/WRITE) → computePeakThroughput() → FALCON_LOG
```

---

## 🟡 Minor Bug #1: RPC 失败时记录持续累积

### 问题描述

```cpp
// falcon_init.cpp reportingThreadFunc
auto records = FalconStats::GetInstance().getRecordsForReport(pid);
int ret = client.ReportIORecords(nodeId, pid, records);
if (ret == 0) {
    FalconStats::GetInstance().cleanupReportedRecords(records);  // 仅成功时清理
}
```

若 RPC 持续失败（网络故障、FUSE 进程未启动），`readRecords`/`writeRecords` 持续增长至 `MAX_IO_RECORDS = 1,280,000`。但达到上限后会被截断（`erase(begin())`），不会无限增长。

**影响**：故障期间可能丢失部分 IO 记录，但系统不会 OOM。

---

## 🟡 Minor Bug #2: checkpoint 计算受 inflight 记录影响 — 可能增加峰值计算数据量

### 问题描述

```cpp
size_t checkpointNs = nowNs - 1000000000;  // 1s 前
for (const auto &kv : typeRecords) {
    if (kv.second.isInflight && kv.second.startTimeNs < checkpointNs) {
        checkpointNs = kv.second.startTimeNs;
    }
}
```

虽然 inflight 记录 10s 超时会被清理（`lastReceiveTimeNs > TEN_SECONDS_NS`），但仍在有效期内的 inflight 记录可能将 checkpoint 回退最多 10s，导致峰值计算需要处理更多已完成记录。

**影响**：在存在长时间 inflight IO 的场景下，每次峰值计算的数据量可能增加，但上限为 10s 内的完成记录，属于轻度性能问题。

---

## 🟢 Trivial #1: 日志级别错误

```cpp
FALCON_LOG(LOG_ERROR) << "instantaneous " << typeName << " throughput peak(bytes/ns): " << peak ...;
```

峰值为正常业务信息，应为 `FALCON_LOG(LOG_INFO)`。（已修复）

---

## 🔴 Major: sweep-line 比例分摊导致高并发场景吞吐峰值被高估

### 问题描述

sweep-line 算法在 `computePeakThroughput()` 中使用时间比例分摊：

```
contribution = bytes × segLen / totalDuration
throughput = Σ contribution / Δt
```

这隐含假设每个 IO 在其 wall-clock 持续时间内以恒定速率传输数据。但在高并发 O_DIRECT 场景下：

1. Linux 块层将并发 `pwrite()` 在设备层面串行化
2. `pwrite()` 的 wall-clock 时间包含大量队列等待，而非实际传输时间
3. 并发 IO 数量为 N 时，比例分摊的峰值 ≈ `ln(N) × 实际磁盘吞吐`

实测场景（8 进程 × 17 并发 = 136 IO），计算出的吞吐峰值约为 fio 实测磁盘吞吐的 5 倍，与 `ln(136) ≈ 4.9` 吻合。

### 修复方案

新增**自适应窗口算法** (`computeAdaptiveThroughput`)，通过 `falcon_io_stats_use_adaptive_window` 开关启用：

- 计算方式：`Σ(最近 N 个 IO 字节数) / (时间跨度)`，不做比例分摊
- 窗口随突发密度自动缩放（由 `falcon_io_stats_adaptive_min_samples` 控制样本数）
- 时间上界由 `falcon_io_stats_adaptive_max_window_sec` 控制
- 与 fio 的 `总字节/时间窗口` 计算方式一致

详见 `docs/io_peak_throughput_design.md` 第 8 节。

---

## 总结

| 维度 | 评估 |
|------|------|
| **架构正确性** | ✅ Store → brpc RPC → FUSE brpc server → IORecordAggregator → peakCalcThread |
| **功能可用性** | ✅ 可正确采集 IO 记录 |
| **增量上报** | ✅ `cleanupReportedRecords` 确保每次只上报新增记录 |
| **sweep-line 算法** | ⚠️ 实现正确，但在高并发场景下比例分摊导致吞吐高估（已通过自适应算法修复） |
| **自适应算法** | ✅ 基于样本数滑动窗口，与 fio 一致，已上线 |
| **线程安全** | ✅ mutex 保护适当 |
| **资源清理** | ✅ inflight 10s 超时清理、completed 记录按 checkpoint/lookback 清理、Store 端 MAX_IO_RECORDS 上限 |
| **高 IOPS 场景** | ✅ 增量上报避免全量拷贝，性能与 IO 速率成正比 |
