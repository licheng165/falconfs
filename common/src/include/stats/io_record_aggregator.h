/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <mutex>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "falcon_stats.h"

namespace std {
template<>
struct hash<std::tuple<int, size_t, size_t>> {
    size_t operator()(const std::tuple<int, size_t, size_t> &t) const noexcept
    {
        return std::hash<int>{}(std::get<0>(t)) ^
               (std::hash<size_t>{}(std::get<1>(t)) << 1) ^
               (std::hash<size_t>{}(std::get<2>(t)) << 2);
    }
};
}

class IORecordAggregator {
  public:
    static IORecordAggregator &GetInstance()
    {
        static IORecordAggregator instance;
        return instance;
    }

    void receiveIORecords(int nodeId, int pid, const std::vector<IORecordForReport> &records);

    void aggregateAndPrintPeak(IOStatsType type);

    double computePeakThroughput(std::vector<IORecordForReport> &records);

  private:
    IORecordAggregator() = default;

    struct AggregatedRecord {
        int pid;
        size_t recordId;
        size_t ioBytes;
        size_t startTimeNs;
        size_t endTimeNs;
        bool isInflight;
        size_t lastReceiveTimeNs;
    };

    using RecordMap = std::unordered_map<std::tuple<int, size_t, size_t>, AggregatedRecord>;

    void aggregateAndPrintPeakForRecords(RecordMap &typeRecords, IOStatsType type);

    std::mutex mutex_;
    RecordMap readRecords_;
    RecordMap writeRecords_;
};
