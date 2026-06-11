/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "stats/io_record_aggregator.h"

#include <algorithm>
#include <chrono>
#include <iostream>

#include "log/logging.h"

void IORecordAggregator::receiveIORecords(int nodeId, int pid, const std::vector<IORecordForReport> &records)
{
    (void)nodeId;

    auto now = std::chrono::steady_clock::now();
    size_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

    std::lock_guard<std::mutex> lock(mutex_);

    for (const auto &r : records) {
        RecordMap &targetRecords =
            (r.ioType == static_cast<int>(IO_WRITE)) ? writeRecords_ : readRecords_;
        auto key = std::make_tuple(r.pid, r.recordId, r.startTimeNs);
        auto it = targetRecords.find(key);

        if (r.isInflight) {
            if (it != targetRecords.end()) {
                it->second.lastReceiveTimeNs = nowNs;
            } else {
                AggregatedRecord ar;
                ar.pid = r.pid;
                ar.recordId = r.recordId;
                ar.ioBytes = 0;
                ar.startTimeNs = r.startTimeNs;
                ar.endTimeNs = 0;
                ar.isInflight = true;
                ar.lastReceiveTimeNs = nowNs;
                targetRecords[key] = ar;
            }
        } else {
            if (it != targetRecords.end()) {
                it->second.endTimeNs = r.endTimeNs;
                it->second.ioBytes = r.ioBytes;
                it->second.isInflight = false;
                it->second.lastReceiveTimeNs = nowNs;
            } else {
                AggregatedRecord ar;
                ar.pid = r.pid;
                ar.recordId = r.recordId;
                ar.ioBytes = r.ioBytes;
                ar.startTimeNs = r.startTimeNs;
                ar.endTimeNs = r.endTimeNs;
                ar.isInflight = false;
                ar.lastReceiveTimeNs = nowNs;
                targetRecords[key] = ar;
            }
        }
    }
}

double IORecordAggregator::computePeakThroughput(std::vector<IORecordForReport> &records)
{
    struct IORecordEvent {
        size_t timeStampNs;
        bool isStart;
        IORecordForReport *record;
    };

    std::vector<IORecordEvent> recordEventList;
    for (auto &record : records) {
        recordEventList.push_back({record.startTimeNs, true, &record});
        recordEventList.push_back({record.endTimeNs, false, &record});
    }

    std::sort(recordEventList.begin(), recordEventList.end(), [](const IORecordEvent &a, const IORecordEvent &b) {
        if (a.timeStampNs == b.timeStampNs) {
            return a.isStart > b.isStart;
        }
        return a.timeStampNs < b.timeStampNs;
    });

    double maxThroughput = 0.0;
    std::unordered_set<IORecordForReport *> activeRecords;

    if (recordEventList.empty()) {
        return 0.0;
    }

    if (recordEventList[0].isStart) {
        activeRecords.insert(recordEventList[0].record);
    }
    for (size_t i = 1; i < recordEventList.size(); i++) {
        IORecordEvent &windowStartEvent = recordEventList[i - 1];
        IORecordEvent &windowEndEvent = recordEventList[i];

        if (windowEndEvent.isStart) {
            activeRecords.insert(windowEndEvent.record);
        }
        if (!windowStartEvent.isStart) {
            activeRecords.erase(windowStartEvent.record);
        }

        if (windowEndEvent.timeStampNs == windowStartEvent.timeStampNs) {
            continue;
        }

        double totalBytes = 0.0;
        for (auto *r : activeRecords) {
            size_t segStart = std::max(r->startTimeNs, windowStartEvent.timeStampNs);
            size_t segEnd = std::min(r->endTimeNs, windowEndEvent.timeStampNs);
            if (segEnd > segStart) {
                double segLen = static_cast<double>(segEnd - segStart);
                double totalLen = static_cast<double>(r->endTimeNs - r->startTimeNs);
                if (totalLen > 0.0) {
                    totalBytes += static_cast<double>(r->ioBytes) * segLen / totalLen;
                }
            }
        }

        double windowLen = windowEndEvent.timeStampNs - windowStartEvent.timeStampNs;
        double throughput = totalBytes / windowLen;

        if (throughput > maxThroughput) {
            maxThroughput = throughput;
        }
    }

    return maxThroughput;
}

double IORecordAggregator::computeAdaptiveThroughput(const std::vector<IORecordForReport> &records,
                                                     uint32_t minSamples, size_t maxWindowNs)
{
    if (records.size() < minSamples) {
        return 0.0;
    }

    size_t windowEnd = records.back().endTimeNs;
    size_t firstIdx = records.size() - minSamples;
    size_t windowStart = records[firstIdx].endTimeNs;

    if (windowStart >= windowEnd) {
        return 0.0;
    }

    size_t timeSpan = windowEnd - windowStart;

    if (timeSpan > maxWindowNs) {
        timeSpan = maxWindowNs;
        size_t cutoff = windowEnd > maxWindowNs ? (windowEnd - maxWindowNs) : 0;
        while (firstIdx < records.size() && records[firstIdx].endTimeNs < cutoff) {
            firstIdx++;
        }
    }

    size_t totalBytes = 0;
    for (size_t i = firstIdx; i < records.size(); i++) {
        totalBytes += records[i].ioBytes;
    }

    return static_cast<double>(totalBytes) / static_cast<double>(timeSpan);
}

void IORecordAggregator::aggregateAndPrintPeakForRecords(RecordMap &typeRecords, IOStatsType type)
{
    auto now = std::chrono::steady_clock::now();
    size_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

    const char *typeName = (type == IO_WRITE) ? "WRITE" : "READ";
    constexpr size_t TEN_SECONDS_NS = 10000000000;

    if (useAdaptiveWindow_) {
        size_t lookbackNs = nowNs > adaptiveLookbackNs_ ? (nowNs - adaptiveLookbackNs_) : 0;

        std::vector<IORecordForReport> completed;
        for (const auto &kv : typeRecords) {
            if (!kv.second.isInflight && kv.second.endTimeNs >= lookbackNs) {
                IORecordForReport r;
                r.pid = kv.second.pid;
                r.recordId = kv.second.recordId;
                r.ioType = static_cast<int>(type);
                r.ioBytes = kv.second.ioBytes;
                r.startTimeNs = kv.second.startTimeNs;
                r.endTimeNs = kv.second.endTimeNs;
                r.isInflight = false;
                completed.push_back(r);
            }
        }

        if (!completed.empty()) {
            std::sort(completed.begin(), completed.end(),
                [](const IORecordForReport &a, const IORecordForReport &b) {
                    return a.endTimeNs < b.endTimeNs;
                });

            double throughput = computeAdaptiveThroughput(completed, adaptiveMinSamples_, adaptiveMaxWindowNs_);

            size_t usedSamples = (completed.size() >= adaptiveMinSamples_) ? adaptiveMinSamples_ : 0;
            size_t usedWindow = 0;
            if (usedSamples > 0) {
                size_t firstIdx = completed.size() - usedSamples;
                usedWindow = completed.back().endTimeNs - completed[firstIdx].endTimeNs;
                if (usedWindow > adaptiveMaxWindowNs_) {
                    usedWindow = adaptiveMaxWindowNs_;
                }
            }

            FALCON_LOG(LOG_ERROR) << "adaptive " << typeName
                                 << " throughput(bytes/ns): " << throughput
                                 << " [samples=" << usedSamples
                                 << ", window=" << usedWindow << "ns]";
        }

        auto it = typeRecords.begin();
        while (it != typeRecords.end()) {
            if (!it->second.isInflight && it->second.endTimeNs < lookbackNs) {
                it = typeRecords.erase(it);
            } else if (it->second.isInflight && (nowNs - it->second.lastReceiveTimeNs > TEN_SECONDS_NS)) {
                it = typeRecords.erase(it);
            } else {
                ++it;
            }
        }
    } else {
        size_t checkpointNs = nowNs - 1000000000;

        for (const auto &kv : typeRecords) {
            if (kv.second.isInflight && kv.second.startTimeNs < checkpointNs) {
                checkpointNs = kv.second.startTimeNs;
            }
        }

        std::vector<IORecordForReport> completedBeforeCheckpoint;
        for (const auto &kv : typeRecords) {
            if (!kv.second.isInflight && kv.second.startTimeNs < checkpointNs) {
                IORecordForReport r;
                r.pid = kv.second.pid;
                r.recordId = kv.second.recordId;
                r.ioType = static_cast<int>(type);
                r.ioBytes = kv.second.ioBytes;
                r.startTimeNs = kv.second.startTimeNs;
                r.endTimeNs = kv.second.endTimeNs;
                r.isInflight = false;
                completedBeforeCheckpoint.push_back(r);
            }
        }

        if (!completedBeforeCheckpoint.empty()) {
            double peak = computePeakThroughput(completedBeforeCheckpoint);
            FALCON_LOG(LOG_ERROR) << "instantaneous " << typeName
                                 << " throughput peak(bytes/ns): " << peak
                                 << ", time window: [" << checkpointNs - 1000000000 << ", " << checkpointNs << "]";
        }

        auto it = typeRecords.begin();
        while (it != typeRecords.end()) {
            if (!it->second.isInflight && it->second.endTimeNs < checkpointNs) {
                it = typeRecords.erase(it);
            } else if (it->second.isInflight && (nowNs - it->second.lastReceiveTimeNs > TEN_SECONDS_NS)) {
                it = typeRecords.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void IORecordAggregator::aggregateAndPrintPeak(IOStatsType type)
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (type == IO_WRITE) {
        aggregateAndPrintPeakForRecords(writeRecords_, IO_WRITE);
    } else {
        aggregateAndPrintPeakForRecords(readRecords_, IO_READ);
    }
}
