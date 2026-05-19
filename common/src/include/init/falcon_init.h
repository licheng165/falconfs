/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <thread>

#include "conf/falcon_config.h"

class FalconModuleInit {
  public:
    explicit FalconModuleInit()
        : falconConfig(nullptr)
    {
    }
    explicit FalconModuleInit(const std::string &conf)
        : falconConfig(nullptr),
          configDir(conf)
    {
    }

    ~FalconModuleInit() = default;

    int32_t Init();
    int32_t InnerInit();
    int32_t InitConf();
    int32_t InitLog();
    std::shared_ptr<FalconConfig> &GetFalconConfig();

    static void SetIsFuseProcess();
    static bool IsFuseProcess();

  protected:
    std::shared_ptr<FalconConfig> falconConfig;
    std::string configDir;
    bool inited = false;
    std::jthread reportingThread;

  private:
    static bool fuseProcess;
};

FalconModuleInit &GetInit();
