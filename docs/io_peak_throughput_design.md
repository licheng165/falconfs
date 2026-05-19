# FalconFS IO Peak Throughput Monitoring — 详细设计方案

## 1. 背景与目标

### 1.1 需求
在 AI 工作负载场景下，FalconFS 需要提供 **IO 瞬时峰值吞吐量** 的监控能力，帮助运维人员：
- 诊断 IO 瓶颈，优化存储分层配置
- 评估当前硬件（DRAM/SSD/对象存储）是否满足业务峰值需求
- 端到端性能分析（配合 LMCache 等上层组件）

### 1.2 设计目标
1. **单次 IO 精度**：记录每次 read/write 操作的起止时间戳和字节数
2. **瞬时峰值计算**：基于滑动时间窗口的 sweep-line 算法，计算任意时间窗口内的最大吞吐量
3. **跨进程聚合**：FUSE 进程与 Store 进程分别位于不同进程，需通过 RPC 汇总 IO 记录
4. **低侵入性**：通过 RAII 模式 (`IOStatDuration`) 最小化对现有代码的侵入
5. **可配置**：支持开关控制和采样间隔配置

---

## 2. 架构设计

### 2.1 组件架构图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        FUSE Process (fuse_main)                              │
│                                                                              │
│  ┌──────────────────────┐   ┌───────────────────────────────────────────┐  │
│  │  FalconStats         │   │  IORecordAggregator (Singleton)            │  │
│  │  (per-process stats) │   │                                            │  │
│  └──────────┬───────────┘   │  ┌─────────────────────────────────────┐  │  │
│             │ self-report   │  │ readRecords_  / writeRecords_       │  │  │
│             │ (every 1s)    │  │ (RecordMap)                         │  │  │
│             ▼               │  └──────────────┬──────────────────────┘  │  │
│  ┌──────────────────┐       │                 │ peakCalcThread          │  │
│  │ getRecordsForRpt  │──────►  receiveIORecords()                       │  │
│  │ cleanupRptRecords │       │                 │ (every N seconds)      │  │
│  └──────────────────┘       │                 ▼                         │  │
│                             │  ┌─────────────────────────────────────┐  │  │
│  ┌──────────────────────────┤  │ aggregateAndPrintPeak(READ/WRITE)  │  │  │
│  │ brpc server:             │  │   └─ computePeakThroughput()       │  │  │
│  │ RemoteIOServiceImpl      │  │      (sweep-line algorithm)        │  │  │
│  │   ReportIORecords() ─────┼──► receiveIORecords()                 │  │  │
│  │   (接收外部 Store RPC)    │  └─────────────────────────────────────┘  │  │
│  └──────────────────────────┘                                             │  │
└──────────────────────────────────────▲──────────────────────────────────────┘
                                       │
                          brpc RPC: ReportIORecords (every 1s)
                          target: clusterView[nodeId] = FUSE endpoint
                                       │
┌──────────────────────────────────────┴──────────────────────────────────────┐
│                     Store Process (separate binary)                         │
│                                                                             │
│  ┌─────────────────────┐     ┌──────────────────────────────────────┐     │
│  │  FalconStats         │     │  reportingThreadFunc()               │     │
│  │                      │     │                                      │     │
│  │  ┌───────────────┐  │     │  getRecordsForReport(pid) ──────►    │     │
│  │  │inflightDurations│  │     │       │                              │     │
│  │  │readRecords     │  │     │       ▼                              │     │
│  │  │writeRecords    │  │     │  client.ReportIORecords()            │     │
│  │  └───────┬───────┘  │     │       │  (brpc → FUSE:56039)          │     │
│  │          │           │     │       ▼                              │     │
│  │    startIO()         │     │  cleanupReportedRecords()            │     │
│  │    finishIO()        │     └──────────────────────────────────────┘     │
│  │    cancelIO()        │                                                   │
│  └──────────────────────┘                                                   │
│                                                                             │
│  Store I/O instrumentation:                                                 │
│    pwrite() ─── IOStatDuration.startIO/finishIO                            │
│    pread()  ─── IOStatDuration.startIO/finishIO                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 数据结构

#### IORecord（内部存储）
```cpp
struct IORecord {
    size_t recordId;    // 全局唯一 ID（原子递增）
    size_t ioBytes;     // IO 字节数
    size_t startTimeNs; // 开始时间戳（ns）
    size_t endTimeNs;   // 结束时间戳（ns）
};
```

#### IORecordForReport（跨进程传输）
```cpp
struct IORecordForReport {
    int pid;            // 所属进程 PID
    size_t recordId;    // 全局唯一 ID
    int ioType;         // IO_READ(0) / IO_WRITE(1)
    size_t ioBytes;     // IO 字节数
    size_t startTimeNs; // 开始时间戳（ns）
    size_t endTimeNs;   // 结束时间戳（ns），inflight 时为 0
    bool isInflight;    // 是否正在进行中
};
```

#### IOStatDuration（RAII 包装器）
```cpp
class IOStatDuration {
    // 构造时不记录任何信息
    // startIO() 填充 durationType, recordId, startTimeNs, stats 指针
    // finishIO() 标记完成
    // 析构时自动 cancelIO（若未 finish）
};
```

---

## 3. 核心算法：瞬时峰值吞吐量计算

### 3.1 问题定义

给定一组已完成 IO 记录 `{R_i = (start_i, end_i, bytes_i)}`，求任意时间区间 `[t, t+Δt]` 内的最大吞吐量：

```
throughput = Σ (bytes_i * overlap_ratio_i) / Δt
```

其中 `overlap_ratio_i = |[start_i, end_i] ∩ [t, t+Δt]| / |end_i - start_i|`，即 IO 在时间窗口内的时长占比。

### 3.2 Sweep-Line 算法

**关键洞察**：最大吞吐量必定出现在相邻事件时间戳之间。因此只需在事件边界处计算吞吐量。

```
Input:  records = [(start, end, bytes), ...]
Output: max_throughput

1. 将每条记录拆分为两个事件：
   - (start, isStart=true, record_ptr)
   - (end,   isStart=false, record_ptr)

2. 按时间戳排序，同时刻 start 事件优先于 end 事件

3. 初始化 active_set = {event[0].record}

4. 遍历 i = 1..N-1:
   a. 若 event[i] 是 start → 插入 active_set
   b. 若 event[i-1] 是 end  → 从 active_set 移除
   c. 若 event[i].time == event[i-1].time → 跳过（窗口长度为 0）
   d. 计算窗口 [event[i-1].time, event[i].time] 内的吞吐量：
      - 对 active_set 中的每条记录 r:
        segStart = max(r.start, event[i-1].time)
        segEnd   = min(r.end,   event[i].time)
        contribution = r.bytes * (segEnd - segStart) / (r.end - r.start)
      - throughput = Σ contribution / (event[i].time - event[i-1].time)
   e. 更新 max_throughput = max(max_throughput, throughput)
```

**复杂度**：O(N log N) 时间（排序），O(N) 空间。

### 3.3 示例

```
记录 A: [0ns, 100ns], 100 bytes
记录 B: [30ns, 70ns],  50 bytes

事件排序：(0,s,A), (30,s,B), (70,e,B), (100,e,A)

i=1: [0,30]:  A=30/100*100=30, B=0 → throughput=30/30=1.0 B/ns
i=2: [30,70]: A=40/100*100=40, B=40/40*50=50 → throughput=90/40=2.25 B/ns  ← peak!
i=3: [70,100]: A=30/100*100=30 → throughput=30/30=1.0 B/ns

max_throughput = 2.25 bytes/ns
```

---

## 4. 跨进程数据流

### 4.1 数据报告流程

```
第二层：峰值计算（仅 FUSE 进程）
  FUSE Process: peakCalcThread (每 N 秒)
    └─ aggregateAndPrintPeak(IO_READ)
    └─ aggregateAndPrintPeak(IO_WRITE)
         ├─ 选择已完成记录（截至最早 inflight 记录之前）
         ├─ computePeakThroughput()  // sweep-line 算法
         └─ 清理过期记录

第一层：记录聚合（FUSE 进程）
  FUSE selfReportThread (每 1 秒):
    └─ getRecordsForReport(getpid()) → receiveIORecords(-1, pid, records)  // 直接调用，无 RPC
    └─ cleanupReportedRecords(records)

  FUSE brpc server (RemoteIOServiceImpl::ReportIORecords):
    └─ 接收外部 Store 的 RPC → receiveIORecords(nodeId, pid, records)
    （Store 的 reportingThread 每 1 秒通过 brpc RPC 发送到此 handler）

第零层：IO 采集（Store 进程 / 合并进程）
  Store Process: pwrite/pread 调用点
    └─ IOStatDuration duration
    └─ FalconStats::startIO(duration, type)
    └─ ... 执行 IO ...
    └─ FalconStats::finishIO(duration, success, bytes)
    └─ ~IOStatDuration() // RAII 自动清理 inflight

  Store reportingThread (每 1 秒, falcon_init.cpp):
    └─ getRecordsForReport(getpid())
    └─ client.ReportIORecords(nodeId, pid)  // brpc RPC → FUSE endpoint
    └─ cleanupReportedRecords(records)
```

### 4.2 Protobuf 定义

```protobuf
message IORecordMsg {
    int32 pid = 1;
    uint64 record_id = 2;
    int32 io_type = 3;       // 0=READ, 1=WRITE
    uint64 io_bytes = 4;
    uint64 start_time_ns = 5;
    uint64 end_time_ns = 6;
    bool is_inflight = 7;
}

message ReportIORecordsRequest {
    int32 node_id = 1;
    int32 pid = 2;
    repeated IORecordMsg records = 3;
}

message ReportIORecordsReply {
    int32 error_code = 1;
}
```

---

## 5. 配置参数

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `falcon_io_stats_report_to_fuse_enable` | bool | true | 是否启用 IO 统计记录 |
| `falcon_io_stats_result_print_interval_sec` | uint | 1 | 峰值计算/打印周期（秒） |

---

## 6. 线程模型

| 线程 | 所属进程 | 周期 | 职责 |
|------|----------|------|------|
| reportingThread | Store (非FUSE) | 1s | 向 FUSE 上报 IO 记录（brpc RPC） |
| selfReportThread | FUSE | 1s | 将本地记录提交到 IORecordAggregator |
| peakCalcThread | FUSE | N s | 计算并打印读写峰值吞吐量 |

---

## 7. 关键设计决策

### 7.1 为什么使用 Sweep-Line 而非固定窗口？

固定窗口（如每秒聚合）会丢失子秒级的瞬时峰值信息。Sweep-line 算法在不丢失精度的前提下，计算任意重叠 IO 的最大瞬时吞吐量。

### 7.2 为什么分离 FalconStats 和 IORecordAggregator？

- **FalconStats**：每个进程独立记录 IO，线程安全，支持 inflight 跟踪
- **IORecordAggregator**：跨节点/跨进程聚合，负责峰值计算和记录清理

### 7.3 inflight 记录的作用

- 报告 inflight 记录让 FUSE 端知道哪些 IO 尚未完成
- 峰值计算时，以最早 inflight 记录的 startTime 为界，避免在数据不完整的时间窗口计算峰值
- 超时（10s）未更新的 inflight 记录会被自动清理

### 7.4 记录清理策略

- **Store 端**：`cleanupReportedRecords` 移除已成功上报到 FUSE 的已完成记录
- **FUSE 端 (aggregator)**：每轮峰值计算后，清理 endTime < checkpoint 的完成记录和超时的 inflight 记录
- **容量上限**：Store 端每类 IO 最多保留 `MAX_IO_RECORDS = 1,280,000` 条

---

## 8. IO 埋点清单

| 文件 | 函数 | IO 类型 |
|------|------|---------|
| `falcon_store.cpp` | `WriteLocalFileForBrpc` | WRITE |
| `falcon_store.cpp` | `ReadFileLR` | READ (含 EAGAIN 重试) |
| `falcon_store.cpp` | `ReadSmallFiles` | READ (含 EAGAIN 重试) |
| `falcon_store.cpp` | `WriteToFileAsync` | WRITE |
| `falcon_store.cpp` | `ReadSmallFilesForBrpc` | READ (含 EAGAIN 重试) |
| `stream_assembler.cpp` | `PersistToFile` | WRITE |
