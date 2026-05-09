/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <liburing.h>
#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <unordered_map>

class IoUringManager {
public:
    static IoUringManager& GetInstance();
    
    int Init(int queueDepth = 256);
    
    void Destroy();
    
    bool IsInitialized() const { return initialized.load(); }
    
    std::future<ssize_t> SubmitRead(int fd, void* buf, size_t size, off_t offset);
    
    std::future<ssize_t> SubmitWrite(int fd, const void* buf, size_t size, off_t offset);
    
private:
    IoUringManager() = default;
    ~IoUringManager();
    
    IoUringManager(const IoUringManager&) = delete;
    IoUringManager& operator=(const IoUringManager&) = delete;
    
    void CompletionThread();
    
    struct io_uring ring;
    int queueDepth = 256;
    std::atomic<bool> initialized{false};
    std::atomic<bool> stop{false};
    
    std::thread completionThread;
    
    struct PendingOp {
        std::promise<ssize_t> promise;
    };
    std::mutex pendingMutex;
    std::unordered_map<void*, PendingOp*> pendingOps;
};
