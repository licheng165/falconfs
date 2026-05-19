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

峰值为正常业务信息，应为 `FALCON_LOG(LOG_INFO)`。

---

## 总结

| 维度 | 评估 |
|------|------|
| **架构正确性** | ✅ Store → brpc RPC → FUSE brpc server → IORecordAggregator → peakCalcThread |
| **功能可用性** | ✅ 可正确采集 IO 记录并计算瞬时峰值吞吐量 |
| **增量上报** | ✅ `cleanupReportedRecords` 确保每次只上报新增记录 |
| **算法正确性** | ✅ sweep-line 算法实现正确 |
| **线程安全** | ✅ mutex 保护适当 |
| **资源清理** | ✅ inflight 10s 超时清理、completed 记录按 checkpoint 清理、Store 端 MAX_IO_RECORDS 上限 |
| **高 IOPS 场景** | ✅ 增量上报避免全量拷贝，性能与 IO 速率成正比 |

**结论**：该实现架构正确、算法正确、线程安全，能够正确且合理地达成"统计 IO 瞬时峰值吞吐量"的目的。无功能性 bug。上述 Minor 问题属于防御性编程和代码质量改进建议。
