/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "init/falcon_init.h"

#include <stop_token>
#include <unistd.h>

#include <brpc/channel.h>

#include "conf/falcon_property_key.h"
#include "connection/falcon_io_client.h"
#include "falcon_code.h"
#include "log/logging.h"
#include "stats/falcon_stats.h"

bool FalconModuleInit::fuseProcess = false;

void FalconModuleInit::SetIsFuseProcess()
{
    fuseProcess = true;
}

bool FalconModuleInit::IsFuseProcess()
{
    return fuseProcess;
}

static void parseClusterViewElement(const std::string &clusterView, int nodeId, std::string &outEndpoint)
{
    size_t start = 0;
    int idx = 0;
    for (size_t i = 0; i <= clusterView.size(); i++) {
        if (i == clusterView.size() || clusterView[i] == ',') {
            if (idx == nodeId) {
                outEndpoint = clusterView.substr(start, i - start);
                return;
            }
            idx++;
            start = i + 1;
        }
    }
}

static void reportingThreadFunc(std::stop_token stoken)
{
    auto &config = GetInit().GetFalconConfig();
    int nodeId = static_cast<int>(config->GetUint32(FalconPropertyKey::FALCON_NODE_ID));
    std::string clusterView = config->GetArray(FalconPropertyKey::FALCON_CLUSTER_VIEW);

    std::string fuseEndpoint;
    parseClusterViewElement(clusterView, nodeId, fuseEndpoint);
    if (fuseEndpoint.empty()) {
        FALCON_LOG(LOG_ERROR) << "IO stats reporting thread: no fuse endpoint found in clusterView";
        return;
    }

    auto channel = std::make_shared<brpc::Channel>();
    brpc::ChannelOptions options;
    if (channel->Init(fuseEndpoint.c_str(), &options) != 0) {
        FALCON_LOG(LOG_ERROR) << "IO stats reporting thread: brpc channel init failed for " << fuseEndpoint;
        return;
    }
    FalconIOClient client(channel);

    int pid = static_cast<int>(getpid());

    while (!stoken.stop_requested()) {
        auto records = FalconStats::GetInstance().getRecordsForReport(pid);
        int ret = client.ReportIORecords(nodeId, pid, records);
        if (ret == 0) {
            FalconStats::GetInstance().cleanupReportedRecords(records);
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

int32_t FalconModuleInit::Init()
{
    if (inited) {
        return FALCON_SUCCESS;
    }
    std::function<int32_t()> falconInitStepOps[] = {[&] { return InnerInit(); },
                                                    [&] { return InitConf(); },
                                                    [&] { return InitLog(); }};

    for (auto &initStep : falconInitStepOps) {
        int32_t ret = initStep();
        if (ret != OK) {
            return FALCON_ERR_INNER_FAILED;
        }
    }
    inited = true;

    bool reportEnabled = falconConfig->GetBool(FalconPropertyKey::FALCON_IO_STATS_REPORT_TO_FUSE_ENABLE);
    FalconStats::GetInstance().setIOStatsEnabled(reportEnabled);

    uint32_t printIntervalSec = falconConfig->GetUint32(FalconPropertyKey::FALCON_IO_STATS_RESULT_PRINT_INTERVAL_SEC);
    if (!IsFuseProcess() && reportEnabled && printIntervalSec != 0) {
        reportingThread = std::jthread(reportingThreadFunc);
    }

    FALCON_LOG(LOG_INFO) << "Init FALCON client successfully";
    return FALCON_SUCCESS;
}

int32_t FalconModuleInit::InnerInit()
{
    if (falconConfig == nullptr) {
        falconConfig = std::make_unique<FalconConfig>();
    }

    return OK;
}

int32_t FalconModuleInit::InitConf()
{
    if (configDir.empty()) {
        return FALCON_IEC_INIT_CONF_FAILED;
    }

    return falconConfig->InitConf(configDir);
}

int32_t FalconModuleInit::InitLog()
{
    auto logMaxSize = falconConfig->GetUint32(FalconPropertyKey::FALCON_LOG_MAX_SIZE_MB);
    auto logDir = falconConfig->GetString(FalconPropertyKey::FALCON_LOG_DIR);
    if (logDir.empty()) {
        logDir = FALCON_DEFAULT_LOG_DIR;
    }

    auto logLevelString = falconConfig->GetString(FalconPropertyKey::FALCON_LOG_LEVEL);
    std::unordered_map<std::string, FalconLogLevel> logLevelMap = {{"TRACE", LOG_TRACE},
                                                                   {"DEBUG", LOG_DEBUG},
                                                                   {"INFO", LOG_INFO},
                                                                   {"WARNING", LOG_WARNING},
                                                                   {"ERROR", LOG_ERROR},
                                                                   {"FATAL", LOG_FATAL}};
    auto logLevel = (logLevelMap.count(logLevelString) ? logLevelMap[logLevelString] : LOG_INFO);

    uint reserved_num = falconConfig->GetUint32(FalconPropertyKey::FALCON_LOG_RESERVED_NUM);

    uint reserved_time = falconConfig->GetUint32(FalconPropertyKey::FALCON_LOG_RESERVED_TIME);

    int32_t ret =
        FalconLog::GetInstance()->InitLog(logLevel, GLOGGER, logDir, "falcon", logMaxSize, reserved_num, reserved_time);
    if (ret != OK) {
        FALCON_LOG(LOG_ERROR) << "Falcon init failed caused by init log failed, error code: " << ret;
        return ret;
    }

    FALCON_LOG(LOG_INFO) << "Init Falcon Log successfully";
    return OK;
}

std::shared_ptr<FalconConfig> &FalconModuleInit::GetFalconConfig() { return falconConfig; }

FalconModuleInit &GetInit()
{
    char *CONFIG_FILE = std::getenv("CONFIG_FILE");
    if (!CONFIG_FILE) {
        FALCON_LOG(LOG_ERROR) << "CONFIG_FILE not set";
    }
    std::string configFile = CONFIG_FILE ? CONFIG_FILE : "";
    static FalconModuleInit instance(configFile);
    return instance;
}
