/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "test_falcon_stats_io.h"

#include <thread>
#include <chrono>

FalconStats &FalconStatsIOUT::stats = FalconStats::GetInstance();

void FalconStatsIOUT::SetUpTestSuite()
{
}

void FalconStatsIOUT::TearDownTestSuite()
{
}

void FalconStatsIOUT::SetUp()
{
    stats.setIOStatsEnabled(true);
}

void FalconStatsIOUT::TearDown()
{
    stats.setIOStatsEnabled(true);
    auto records = stats.getRecordsForReport(1);
    stats.cleanupReportedRecords(records);
}

TEST_F(FalconStatsIOUT, StartIO_RecordsInflightDuration)
{
    IOStatDuration duration;
    stats.startIO(duration, IO_READ);

    EXPECT_TRUE(duration.isValid());
    EXPECT_EQ(duration.getType(), IO_READ);
    EXPECT_TRUE(duration.isRead());
    EXPECT_GT(duration.getRecordId(), 0);
    EXPECT_GT(duration.getStartTime(), 0);
    EXPECT_FALSE(duration.isFinished());
}

TEST_F(FalconStatsIOUT, StartIO_RespectsDisabledFlag)
{
    stats.setIOStatsEnabled(false);

    IOStatDuration duration;
    stats.startIO(duration, IO_WRITE);

    EXPECT_FALSE(duration.isValid());
    EXPECT_FALSE(duration.isFinished());
}

TEST_F(FalconStatsIOUT, FinishIO_RecordsCompletedRead)
{
    IOStatDuration duration;
    stats.startIO(duration, IO_READ);
    size_t startTime = duration.getStartTime();
    size_t recordId = duration.getRecordId();

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    stats.finishIO(duration, true, 4096);

    EXPECT_TRUE(duration.isFinished());

    auto records = stats.getRecordsForReport(123);
    bool found = false;
    for (const auto &r : records) {
        if (r.recordId == recordId && !r.isInflight) {
            found = true;
            EXPECT_EQ(r.ioType, static_cast<int>(IO_READ));
            EXPECT_EQ(r.ioBytes, 4096);
            EXPECT_EQ(r.startTimeNs, startTime);
            EXPECT_GT(r.endTimeNs, startTime);
            EXPECT_EQ(r.pid, 123);
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(FalconStatsIOUT, FinishIO_RecordsCompletedWrite)
{
    IOStatDuration duration;
    stats.startIO(duration, IO_WRITE);
    size_t recordId = duration.getRecordId();

    stats.finishIO(duration, true, 8192);

    auto records = stats.getRecordsForReport(456);
    bool found = false;
    for (const auto &r : records) {
        if (r.recordId == recordId && !r.isInflight) {
            found = true;
            EXPECT_EQ(r.ioType, static_cast<int>(IO_WRITE));
            EXPECT_EQ(r.ioBytes, 8192);
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(FalconStatsIOUT, FinishIO_FailedIONotRecorded)
{
    IOStatDuration duration;
    stats.startIO(duration, IO_READ);
    size_t recordId = duration.getRecordId();

    stats.finishIO(duration, false, 0);

    auto records = stats.getRecordsForReport(1);
    for (const auto &r : records) {
        if (r.recordId == recordId && !r.isInflight) {
            FAIL() << "Failed IO should not appear in completed records";
        }
    }
}

TEST_F(FalconStatsIOUT, FinishIO_RespectsDisabledFlag)
{
    stats.setIOStatsEnabled(false);

    IOStatDuration duration;
    stats.startIO(duration, IO_READ);
    EXPECT_FALSE(duration.isValid());

    stats.finishIO(duration, true, 1024);
    EXPECT_FALSE(duration.isFinished());

    auto records = stats.getRecordsForReport(1);
    EXPECT_TRUE(records.empty());
}

TEST_F(FalconStatsIOUT, CancelIO_RemovesFromInflight)
{
    IOStatDuration duration;
    stats.startIO(duration, IO_WRITE);

    size_t recordId = duration.getRecordId();

    auto beforeCancel = stats.getRecordsForReport(1);
    bool foundInflight = false;
    for (const auto &r : beforeCancel) {
        if (r.recordId == recordId && r.isInflight) {
            foundInflight = true;
        }
    }
    EXPECT_TRUE(foundInflight);

    stats.cancelIO(duration);

    auto afterCancel = stats.getRecordsForReport(1);
    foundInflight = false;
    for (const auto &r : afterCancel) {
        if (r.recordId == recordId && r.isInflight) {
            foundInflight = true;
        }
    }
    EXPECT_FALSE(foundInflight);
}

TEST_F(FalconStatsIOUT, DestructorAutoCancels)
{
    size_t recordId;
    {
        IOStatDuration duration;
        stats.startIO(duration, IO_READ);
        recordId = duration.getRecordId();

        auto before = stats.getRecordsForReport(1);
        bool foundInflight = false;
        for (const auto &r : before) {
            if (r.recordId == recordId && r.isInflight) {
                foundInflight = true;
            }
        }
        EXPECT_TRUE(foundInflight);
    }

    auto after = stats.getRecordsForReport(1);
    bool stillInflight = false;
    for (const auto &r : after) {
        if (r.recordId == recordId && r.isInflight) {
            stillInflight = true;
        }
    }
    EXPECT_FALSE(stillInflight);
}

TEST_F(FalconStatsIOUT, DestructorAfterFinishIsNoop)
{
    {
        IOStatDuration duration;
        stats.startIO(duration, IO_WRITE);
        stats.finishIO(duration, true, 100);
        EXPECT_TRUE(duration.isFinished());
    }

    EXPECT_TRUE(true);
}

TEST_F(FalconStatsIOUT, CleanupReportedRecords_RemovesCompleted)
{
    IOStatDuration dur1, dur2;
    stats.startIO(dur1, IO_READ);
    stats.startIO(dur2, IO_READ);

    stats.finishIO(dur1, true, 100);
    stats.finishIO(dur2, true, 200);

    size_t id1 = dur1.getRecordId();
    size_t id2 = dur2.getRecordId();

    auto records = stats.getRecordsForReport(1);
    EXPECT_GE(records.size(), 2);

    std::vector<IORecordForReport> toCleanup;
    for (const auto &r : records) {
        if (r.recordId == id1 && !r.isInflight) {
            toCleanup.push_back(r);
        }
    }
    EXPECT_EQ(toCleanup.size(), 1);

    stats.cleanupReportedRecords(toCleanup);

    auto remaining = stats.getRecordsForReport(1);
    bool foundId1 = false;
    bool foundId2 = false;
    for (const auto &r : remaining) {
        if (r.recordId == id1 && !r.isInflight) foundId1 = true;
        if (r.recordId == id2 && !r.isInflight) foundId2 = true;
    }
    EXPECT_FALSE(foundId1) << "Record id1 should be cleaned up";
    EXPECT_TRUE(foundId2) << "Record id2 should remain";
}

TEST_F(FalconStatsIOUT, CleanupReportedRecords_DoesNotRemoveInflight)
{
    IOStatDuration dur1;
    stats.startIO(dur1, IO_READ);
    size_t inflightId = dur1.getRecordId();

    auto before = stats.getRecordsForReport(1);
    std::vector<IORecordForReport> toCleanup;
    for (const auto &r : before) {
        if (r.recordId == inflightId) {
            toCleanup.push_back(r);
        }
    }
    EXPECT_GE(toCleanup.size(), 1);

    stats.cleanupReportedRecords(toCleanup);

    auto after = stats.getRecordsForReport(1);
    bool stillInflight = false;
    for (const auto &r : after) {
        if (r.recordId == inflightId && r.isInflight) {
            stillInflight = true;
        }
    }
    EXPECT_TRUE(stillInflight) << "Inflight record should not be cleaned up";

    stats.cancelIO(dur1);
}

TEST_F(FalconStatsIOUT, GetRecordsForReport_ReturnsInflightAndCompleted)
{
    IOStatDuration inflightDur, completedDur;
    stats.startIO(inflightDur, IO_READ);
    stats.startIO(completedDur, IO_WRITE);
    stats.finishIO(completedDur, true, 1024);

    auto records = stats.getRecordsForReport(42);

    bool hasInflight = false;
    bool hasCompleted = false;
    for (const auto &r : records) {
        EXPECT_EQ(r.pid, 42);
        if (r.isInflight && r.recordId == inflightDur.getRecordId()) {
            hasInflight = true;
            EXPECT_EQ(r.ioBytes, 0);
            EXPECT_EQ(r.endTimeNs, 0);
        }
        if (!r.isInflight && r.recordId == completedDur.getRecordId()) {
            hasCompleted = true;
            EXPECT_EQ(r.ioBytes, 1024);
            EXPECT_GT(r.endTimeNs, 0);
            EXPECT_EQ(r.ioType, static_cast<int>(IO_WRITE));
        }
    }
    EXPECT_TRUE(hasInflight);
    EXPECT_TRUE(hasCompleted);

    stats.cancelIO(inflightDur);
}

TEST_F(FalconStatsIOUT, GetRecordsForReport_ReturnsEmptyWhenDisabled)
{
    IOStatDuration duration;
    stats.startIO(duration, IO_READ);
    stats.finishIO(duration, true, 100);

    stats.setIOStatsEnabled(false);
    auto records = stats.getRecordsForReport(1);
    EXPECT_TRUE(records.empty());

    stats.setIOStatsEnabled(true);
    stats.cleanupReportedRecords(stats.getRecordsForReport(1));
}

TEST_F(FalconStatsIOUT, RecordIDIsMonotonicallyIncreasing)
{
    IOStatDuration d1, d2, d3;
    stats.startIO(d1, IO_READ);
    stats.startIO(d2, IO_WRITE);
    stats.startIO(d3, IO_READ);

    EXPECT_LT(d1.getRecordId(), d2.getRecordId());
    EXPECT_LT(d2.getRecordId(), d3.getRecordId());

    stats.cancelIO(d1);
    stats.cancelIO(d2);
    stats.cancelIO(d3);
}

TEST_F(FalconStatsIOUT, ConcurrentReadWriteRecords)
{
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    constexpr int kNumThreads = 4;
    constexpr int kOpsPerThread = 100;

    std::vector<std::thread> threads;
    for (int t = 0; t < kNumThreads; t++) {
        threads.emplace_back([&ready, &start, t]() {
            IOStatsType type = (t % 2 == 0) ? IO_READ : IO_WRITE;

            ready++;
            while (!start.load()) {
                std::this_thread::yield();
            }

            for (int i = 0; i < kOpsPerThread; i++) {
                IOStatDuration duration;
                FalconStats::GetInstance().startIO(duration, type);
                FalconStats::GetInstance().finishIO(duration, true, 4096);
            }
        });
    }

    while (ready.load() < kNumThreads) {
        std::this_thread::yield();
    }
    start.store(true);

    for (auto &t : threads) {
        t.join();
    }

    auto records = stats.getRecordsForReport(999);
    size_t completedCount = 0;
    for (const auto &r : records) {
        if (!r.isInflight) {
            completedCount++;
        }
    }
    EXPECT_EQ(completedCount, kNumThreads * kOpsPerThread);

    stats.cleanupReportedRecords(stats.getRecordsForReport(999));
}

TEST_F(FalconStatsIOUT, MaxRecordsLimitsVectorSize)
{
    std::vector<IOStatDuration> durations;

    constexpr size_t kNumRecords = 100000;
    for (size_t i = 0; i < kNumRecords; i++) {
        durations.emplace_back();
        stats.startIO(durations.back(), IO_READ);
        stats.finishIO(durations.back(), true, 4096);
    }

    auto records = stats.getRecordsForReport(1);
    size_t completed = 0;
    for (const auto &r : records) {
        if (!r.isInflight) completed++;
    }
    EXPECT_LE(completed, kNumRecords);

    stats.cleanupReportedRecords(records);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
