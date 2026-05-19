/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "stats/io_record_aggregator.h"
#include "stats/falcon_stats.h"

class IORecordAggregatorUT : public testing::Test {
  public:
    IORecordAggregatorUT();
    void SetUp() override;
    void TearDown() override;

    IORecordAggregator &aggregator;

  protected:
    void AddCompletedRecord(int pid, size_t recordId, IOStatsType type,
                            size_t ioBytes, size_t startTimeNs, size_t endTimeNs);
    void AddInflightRecord(int pid, size_t recordId, IOStatsType type,
                           size_t startTimeNs);
    void CompleteInflightRecord(int pid, size_t recordId, IOStatsType type,
                                size_t ioBytes, size_t endTimeNs);
};
