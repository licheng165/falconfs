/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "stats/falcon_stats.h"

class FalconStatsIOUT : public testing::Test {
  public:
    static void SetUpTestSuite();
    static void TearDownTestSuite();

    void SetUp() override;
    void TearDown() override;

    static FalconStats &stats;
};
