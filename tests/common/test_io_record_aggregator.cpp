/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "test_io_record_aggregator.h"

#include <cmath>
#include <thread>
#include <chrono>

IORecordAggregatorUT::IORecordAggregatorUT()
    : aggregator(IORecordAggregator::GetInstance())
{
}

void IORecordAggregatorUT::SetUp()
{
}

void IORecordAggregatorUT::TearDown()
{
}

void IORecordAggregatorUT::AddCompletedRecord(int pid, size_t recordId, IOStatsType type,
                                               size_t ioBytes, size_t startTimeNs, size_t endTimeNs)
{
    std::vector<IORecordForReport> records;

    IORecordForReport inflight;
    inflight.pid = pid;
    inflight.recordId = recordId;
    inflight.ioType = static_cast<int>(type);
    inflight.ioBytes = 0;
    inflight.startTimeNs = startTimeNs;
    inflight.endTimeNs = 0;
    inflight.isInflight = true;
    records.push_back(inflight);

    IORecordForReport completed;
    completed.pid = pid;
    completed.recordId = recordId;
    completed.ioType = static_cast<int>(type);
    completed.ioBytes = ioBytes;
    completed.startTimeNs = startTimeNs;
    completed.endTimeNs = endTimeNs;
    completed.isInflight = false;
    records.push_back(completed);

    aggregator.receiveIORecords(0, pid, records);
}

void IORecordAggregatorUT::AddInflightRecord(int pid, size_t recordId, IOStatsType type,
                                              size_t startTimeNs)
{
    std::vector<IORecordForReport> records;
    IORecordForReport r;
    r.pid = pid;
    r.recordId = recordId;
    r.ioType = static_cast<int>(type);
    r.ioBytes = 0;
    r.startTimeNs = startTimeNs;
    r.endTimeNs = 0;
    r.isInflight = true;
    records.push_back(r);
    aggregator.receiveIORecords(0, pid, records);
}

void IORecordAggregatorUT::CompleteInflightRecord(int pid, size_t recordId, IOStatsType type,
                                                   size_t ioBytes, size_t endTimeNs)
{
    std::vector<IORecordForReport> records;
    IORecordForReport r;
    r.pid = pid;
    r.recordId = recordId;
    r.ioType = static_cast<int>(type);
    r.ioBytes = ioBytes;
    r.startTimeNs = 0;
    r.endTimeNs = endTimeNs;
    r.isInflight = false;
    records.push_back(r);
    aggregator.receiveIORecords(0, pid, records);
}

TEST_F(IORecordAggregatorUT, ReceiveInflightThenComplete)
{
    int pid = 100;
    size_t recordId = 1;
    size_t startNs = 1000000;
    size_t endNs = 2000000;

    AddInflightRecord(pid, recordId, IO_READ, startNs);
    CompleteInflightRecord(pid, recordId, IO_READ, 4096, endNs);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_SingleRecord)
{
    std::vector<IORecordForReport> records;
    IORecordForReport r;
    r.pid = 1;
    r.recordId = 1;
    r.ioType = static_cast<int>(IO_READ);
    r.ioBytes = 1000;
    r.startTimeNs = 0;
    r.endTimeNs = 100;
    r.isInflight = false;
    records.push_back(r);

    double peak = aggregator.computePeakThroughput(records);

    double expected = 1000.0 / 100.0;
    EXPECT_DOUBLE_EQ(peak, expected);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_NonOverlappingRecords)
{
    std::vector<IORecordForReport> records;

    IORecordForReport r1;
    r1.pid = 1; r1.recordId = 1; r1.ioType = static_cast<int>(IO_READ);
    r1.ioBytes = 1000; r1.startTimeNs = 0; r1.endTimeNs = 100; r1.isInflight = false;
    records.push_back(r1);

    IORecordForReport r2;
    r2.pid = 1; r2.recordId = 2; r2.ioType = static_cast<int>(IO_READ);
    r2.ioBytes = 2000; r2.startTimeNs = 200; r2.endTimeNs = 300; r2.isInflight = false;
    records.push_back(r2);

    double peak = aggregator.computePeakThroughput(records);

    double maxSingle = std::max(1000.0 / 100.0, 2000.0 / 100.0);
    EXPECT_DOUBLE_EQ(peak, maxSingle);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_OverlappingRecords)
{
    std::vector<IORecordForReport> records;

    IORecordForReport r1;
    r1.pid = 1; r1.recordId = 1; r1.ioType = static_cast<int>(IO_READ);
    r1.ioBytes = 100; r1.startTimeNs = 0; r1.endTimeNs = 100; r1.isInflight = false;
    records.push_back(r1);

    IORecordForReport r2;
    r2.pid = 1; r2.recordId = 2; r2.ioType = static_cast<int>(IO_READ);
    r2.ioBytes = 50; r2.startTimeNs = 30; r2.endTimeNs = 70; r2.isInflight = false;
    records.push_back(r2);

    double peak = aggregator.computePeakThroughput(records);

    double expectedPeak = 2.25;
    EXPECT_DOUBLE_EQ(peak, expectedPeak);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_BackToBackRecords)
{
    std::vector<IORecordForReport> records;

    IORecordForReport r1;
    r1.pid = 1; r1.recordId = 1; r1.ioType = static_cast<int>(IO_READ);
    r1.ioBytes = 50; r1.startTimeNs = 0; r1.endTimeNs = 50; r1.isInflight = false;
    records.push_back(r1);

    IORecordForReport r2;
    r2.pid = 1; r2.recordId = 2; r2.ioType = static_cast<int>(IO_READ);
    r2.ioBytes = 100; r2.startTimeNs = 50; r2.endTimeNs = 100; r2.isInflight = false;
    records.push_back(r2);

    double peak = aggregator.computePeakThroughput(records);

    double expectedPeak = std::max(50.0 / 50.0, 100.0 / 50.0);
    EXPECT_DOUBLE_EQ(peak, expectedPeak);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_BothReadAndWriteRecords)
{
    std::vector<IORecordForReport> records;

    IORecordForReport read1;
    read1.pid = 1; read1.recordId = 1; read1.ioType = static_cast<int>(IO_READ);
    read1.ioBytes = 1000; read1.startTimeNs = 0; read1.endTimeNs = 50; read1.isInflight = false;
    records.push_back(read1);

    IORecordForReport write1;
    write1.pid = 1; write1.recordId = 2; write1.ioType = static_cast<int>(IO_WRITE);
    write1.ioBytes = 2000; write1.startTimeNs = 0; write1.endTimeNs = 50; write1.isInflight = false;
    records.push_back(write1);

    double peak = aggregator.computePeakThroughput(records);

    double expected = (1000.0 + 2000.0) / 50.0;
    EXPECT_DOUBLE_EQ(peak, expected);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_PartialOverlap)
{
    std::vector<IORecordForReport> records;

    IORecordForReport r1;
    r1.pid = 1; r1.recordId = 1; r1.ioType = static_cast<int>(IO_READ);
    r1.ioBytes = 100; r1.startTimeNs = 0; r1.endTimeNs = 100; r1.isInflight = false;
    records.push_back(r1);

    IORecordForReport r2;
    r2.pid = 1; r2.recordId = 2; r2.ioType = static_cast<int>(IO_READ);
    r2.ioBytes = 80; r2.startTimeNs = 50; r2.endTimeNs = 150; r2.isInflight = false;
    records.push_back(r2);

    double peak = aggregator.computePeakThroughput(records);

    double overlapThroughput = (100.0 * 50.0 / 100.0 + 80.0 * 50.0 / 100.0) / 50.0;
    double r1OnlyThroughput = 100.0 * 50.0 / 100.0 / 50.0;
    double r2OnlyThroughput = 80.0 * 50.0 / 100.0 / 50.0;

    double expectedPeak = std::max({r1OnlyThroughput, overlapThroughput, r2OnlyThroughput});
    EXPECT_DOUBLE_EQ(peak, expectedPeak);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_EmptyRecords)
{
    std::vector<IORecordForReport> records;
    double peak = aggregator.computePeakThroughput(records);
    EXPECT_DOUBLE_EQ(peak, 0.0);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_ZeroDurationRecord)
{
    std::vector<IORecordForReport> records;

    IORecordForReport r1;
    r1.pid = 1; r1.recordId = 1; r1.ioType = static_cast<int>(IO_READ);
    r1.ioBytes = 100; r1.startTimeNs = 50; r1.endTimeNs = 50; r1.isInflight = false;
    records.push_back(r1);

    IORecordForReport r2;
    r2.pid = 1; r2.recordId = 2; r2.ioType = static_cast<int>(IO_READ);
    r2.ioBytes = 200; r2.startTimeNs = 0; r2.endTimeNs = 100; r2.isInflight = false;
    records.push_back(r2);

    double peak = aggregator.computePeakThroughput(records);
    double expectedPeak = 200.0 / 100.0;
    EXPECT_DOUBLE_EQ(peak, expectedPeak);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_LargeRecordConcurrentWithSmall)
{
    std::vector<IORecordForReport> records;

    IORecordForReport r1;
    r1.pid = 1; r1.recordId = 1; r1.ioType = static_cast<int>(IO_WRITE);
    r1.ioBytes = 1000000; r1.startTimeNs = 0; r1.endTimeNs = 100; r1.isInflight = false;
    records.push_back(r1);

    IORecordForReport r2;
    r2.pid = 1; r2.recordId = 2; r2.ioType = static_cast<int>(IO_WRITE);
    r2.ioBytes = 10; r2.startTimeNs = 50; r2.endTimeNs = 60; r2.isInflight = false;
    records.push_back(r2);

    double peak = aggregator.computePeakThroughput(records);
    EXPECT_GT(peak, 0.0);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_IdenticalTimingRecords)
{
    std::vector<IORecordForReport> records;
    for (size_t i = 0; i < 10; i++) {
        IORecordForReport r;
        r.pid = 1;
        r.recordId = i + 1;
        r.ioType = static_cast<int>(IO_READ);
        r.ioBytes = 1000;
        r.startTimeNs = 0;
        r.endTimeNs = 100;
        r.isInflight = false;
        records.push_back(r);
    }

    double peak = aggregator.computePeakThroughput(records);

    double expectedPeak = (1000.0 * 10.0) / 100.0;
    EXPECT_DOUBLE_EQ(peak, expectedPeak);
}

TEST_F(IORecordAggregatorUT, ReceiveInflightUpdateTracksTimestamp)
{
    int pid = 42;
    size_t recordId = 42;
    size_t startNs = 1000000;

    AddInflightRecord(pid, recordId, IO_WRITE, startNs);

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    AddInflightRecord(pid, recordId, IO_WRITE, startNs);
}

TEST_F(IORecordAggregatorUT, ComputePeakThroughput_NumericalStability)
{
    std::vector<IORecordForReport> records;

    size_t hugeBytes = static_cast<size_t>(1) << 50;
    size_t hugeDuration = static_cast<size_t>(1) << 40;

    IORecordForReport r;
    r.pid = 1; r.recordId = 1; r.ioType = static_cast<int>(IO_READ);
    r.ioBytes = hugeBytes; r.startTimeNs = 0; r.endTimeNs = hugeDuration;
    r.isInflight = false;
    records.push_back(r);

    double peak = aggregator.computePeakThroughput(records);
    double expected = static_cast<double>(hugeBytes) / static_cast<double>(hugeDuration);
    EXPECT_DOUBLE_EQ(peak, expected);
}

TEST_F(IORecordAggregatorUT, ComputeAdaptiveThroughput_EmptyRecords)
{
    std::vector<IORecordForReport> records;
    double throughput = aggregator.computeAdaptiveThroughput(records, 64, 2000000000);
    EXPECT_DOUBLE_EQ(throughput, 0.0);
}

TEST_F(IORecordAggregatorUT, ComputeAdaptiveThroughput_MinSamplesNotMet)
{
    std::vector<IORecordForReport> records;
    for (size_t i = 0; i < 5; i++) {
        IORecordForReport r;
        r.pid = 1; r.recordId = i; r.ioType = static_cast<int>(IO_WRITE);
        r.ioBytes = 1000; r.startTimeNs = 0;
        r.endTimeNs = (i + 1) * 1000000; r.isInflight = false;
        records.push_back(r);
    }

    double throughput = aggregator.computeAdaptiveThroughput(records, 10, 2000000000);
    EXPECT_DOUBLE_EQ(throughput, 0.0);
}

TEST_F(IORecordAggregatorUT, ComputeAdaptiveThroughput_Basic)
{
    std::vector<IORecordForReport> records;
    for (size_t i = 0; i < 100; i++) {
        IORecordForReport r;
        r.pid = 1; r.recordId = i; r.ioType = static_cast<int>(IO_WRITE);
        r.ioBytes = 1000; r.startTimeNs = 0;
        r.endTimeNs = (i + 1) * 1000000; r.isInflight = false;
        records.push_back(r);
    }

    uint32_t minSamples = 64;
    double throughput = aggregator.computeAdaptiveThroughput(records, minSamples, 2000000000);

    size_t firstIdx = records.size() - minSamples;
    size_t totalBytes = 0;
    for (size_t i = firstIdx; i < records.size(); i++) {
        totalBytes += records[i].ioBytes;
    }
    double expected = static_cast<double>(totalBytes) /
                      static_cast<double>(records.back().endTimeNs - records[firstIdx].endTimeNs);
    EXPECT_DOUBLE_EQ(throughput, expected);
}

TEST_F(IORecordAggregatorUT, ComputeAdaptiveThroughput_SparseRecordsHitCap)
{
    std::vector<IORecordForReport> records;
    for (size_t i = 0; i < 100; i++) {
        IORecordForReport r;
        r.pid = 1; r.recordId = i; r.ioType = static_cast<int>(IO_WRITE);
        r.ioBytes = 1000; r.startTimeNs = 0;
        r.endTimeNs = (i + 1) * 100000000ULL; r.isInflight = false;
        records.push_back(r);
    }

    size_t maxWindowNs = 2000000000;
    double throughput = aggregator.computeAdaptiveThroughput(records, 64, maxWindowNs);

    size_t cutoff = records.back().endTimeNs > maxWindowNs ? (records.back().endTimeNs - maxWindowNs) : 0;
    size_t totalBytes = 0;
    for (const auto &r : records) {
        if (r.endTimeNs >= cutoff) {
            totalBytes += r.ioBytes;
        }
    }
    double expected = static_cast<double>(totalBytes) / static_cast<double>(maxWindowNs);
    EXPECT_DOUBLE_EQ(throughput, expected);
}

TEST_F(IORecordAggregatorUT, ComputeAdaptiveThroughput_DenseBurst)
{
    std::vector<IORecordForReport> records;
    for (size_t i = 0; i < 200; i++) {
        IORecordForReport r;
        r.pid = 1; r.recordId = i; r.ioType = static_cast<int>(IO_WRITE);
        r.ioBytes = 4096; r.startTimeNs = i * 5000;
        r.endTimeNs = i * 5000 + 10000; r.isInflight = false;
        records.push_back(r);
    }

    uint32_t minSamples = 64;
    double throughput = aggregator.computeAdaptiveThroughput(records, minSamples, 2000000000);

    size_t firstIdx = records.size() - minSamples;
    size_t timeSpan = records.back().endTimeNs - records[firstIdx].endTimeNs;
    double expected = static_cast<double>(minSamples * 4096) / static_cast<double>(timeSpan);
    EXPECT_DOUBLE_EQ(throughput, expected);
}

TEST_F(IORecordAggregatorUT, ComputeAdaptiveThroughput_IdenticalEndTime)
{
    std::vector<IORecordForReport> records;
    for (size_t i = 0; i < 100; i++) {
        IORecordForReport r;
        r.pid = 1; r.recordId = i; r.ioType = static_cast<int>(IO_WRITE);
        r.ioBytes = 1000; r.startTimeNs = 0;
        r.endTimeNs = 1000000; r.isInflight = false;
        records.push_back(r);
    }

    double throughput = aggregator.computeAdaptiveThroughput(records, 64, 2000000000);
    EXPECT_DOUBLE_EQ(throughput, 0.0);
}

TEST_F(IORecordAggregatorUT, ComputeAdaptiveThroughput_ConcurrentAccuracy)
{
    const size_t concurrentCount = 136;
    const size_t bytesPerIO = 1048576;

    std::vector<IORecordForReport> records;
    for (size_t i = 0; i < concurrentCount; i++) {
        IORecordForReport r;
        r.pid = 1; r.recordId = i; r.ioType = static_cast<int>(IO_WRITE);
        r.ioBytes = bytesPerIO; r.startTimeNs = 0;
        r.endTimeNs = (i + 1) * 1000000; r.isInflight = false;
        records.push_back(r);
    }

    uint32_t minSamples = 64;
    size_t maxWindowNs = 2000000000;

    double adaptive = aggregator.computeAdaptiveThroughput(records, minSamples, maxWindowNs);

    double sweepLine = aggregator.computePeakThroughput(records);

    double actualDiskThroughput = static_cast<double>(concurrentCount * bytesPerIO) /
                                  static_cast<double>(records.back().endTimeNs - records.front().startTimeNs);

    EXPECT_NEAR(adaptive, actualDiskThroughput, actualDiskThroughput * 0.1);

    EXPECT_GT(sweepLine, adaptive * 2.0);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
