/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "falcon_store/io_uring_manager.h"
#include "log/logging.h"

IoUringManager& IoUringManager::GetInstance() {
    static IoUringManager instance;
    return instance;
}

IoUringManager::~IoUringManager() {
    Destroy();
}

int IoUringManager::Init(int depth) {
    if (initialized.load()) {
        return 0;
    }
    
    queueDepth = depth;
    
    int ret = io_uring_queue_init(queueDepth, &ring, 0);
    if (ret < 0) {
        FALCON_LOG(LOG_ERROR) << "IoUringManager::Init() failed: " << strerror(-ret);
        return ret;
    }
    
    stop.store(false);
    completionThread = std::thread(&IoUringManager::CompletionThread, this);
    initialized.store(true);
    
    FALCON_LOG(LOG_INFO) << "IoUringManager initialized with queue depth: " << queueDepth;
    return 0;
}

void IoUringManager::Destroy() {
    if (!initialized.load()) {
        return;
    }
    
    stop.store(true);
    
    if (completionThread.joinable()) {
        completionThread.join();
    }
    
    {
        std::lock_guard<std::mutex> lock(pendingMutex);
        for (auto& pair : pendingOps) {
            pair.second->promise.set_value(-ECANCELED);
            delete pair.second;
        }
        pendingOps.clear();
    }
    
    io_uring_queue_exit(&ring);
    initialized.store(false);
    
    FALCON_LOG(LOG_INFO) << "IoUringManager destroyed";
}

std::future<ssize_t> IoUringManager::SubmitRead(int fd, void* buf, size_t size, off_t offset) {
    PendingOp* op = new PendingOp{};
    auto future = op->promise.get_future();
    
    if (!initialized.load()) {
        op->promise.set_value(-ENODEV);
        return future;
    }
    
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        op->promise.set_value(-EAGAIN);
        delete op;
        FALCON_LOG(LOG_ERROR) << "IoUringManager::SubmitRead() failed to get SQE";
        return future;
    }
    
    io_uring_prep_read(sqe, fd, buf, size, offset);
    io_uring_sqe_set_data(sqe, op);
    
    {
        std::lock_guard<std::mutex> lock(pendingMutex);
        pendingOps[op] = op;
    }
    
    int ret = io_uring_submit(&ring);
    if (ret < 0) {
        std::lock_guard<std::mutex> lock(pendingMutex);
        pendingOps.erase(op);
        op->promise.set_value(-ret);
        delete op;
        FALCON_LOG(LOG_ERROR) << "IoUringManager::SubmitRead() submit failed: " << strerror(-ret);
    }
    
    return future;
}

std::future<ssize_t> IoUringManager::SubmitWrite(int fd, const void* buf, size_t size, off_t offset) {
    PendingOp* op = new PendingOp{};
    auto future = op->promise.get_future();
    
    if (!initialized.load()) {
        op->promise.set_value(-ENODEV);
        return future;
    }
    
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        op->promise.set_value(-EAGAIN);
        delete op;
        FALCON_LOG(LOG_ERROR) << "IoUringManager::SubmitWrite() failed to get SQE";
        return future;
    }
    
    io_uring_prep_write(sqe, fd, buf, size, offset);
    io_uring_sqe_set_data(sqe, op);
    
    {
        std::lock_guard<std::mutex> lock(pendingMutex);
        pendingOps[op] = op;
    }
    
    int ret = io_uring_submit(&ring);
    if (ret < 0) {
        std::lock_guard<std::mutex> lock(pendingMutex);
        pendingOps.erase(op);
        op->promise.set_value(-ret);
        delete op;
        FALCON_LOG(LOG_ERROR) << "IoUringManager::SubmitWrite() submit failed: " << strerror(-ret);
    }
    
    return future;
}

void IoUringManager::CompletionThread() {
    while (!stop.load()) {
        struct io_uring_cqe* cqe;
        
        struct __kernel_timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 100000000;  // 100ms
        
        int ret = io_uring_wait_cqe_timeout(&ring, &cqe, &ts);
        if (ret == -ETIME || ret == -EINTR) {
            continue;
        }
        if (ret < 0) {
            if (!stop.load()) {
                FALCON_LOG(LOG_ERROR) << "IoUringManager::CompletionThread() wait failed: " << strerror(-ret);
            }
            continue;
        }
        
        ssize_t res = cqe->res;
        PendingOp* op = static_cast<PendingOp*>(io_uring_cqe_get_data(cqe));
        
        if (op) {
            {
                std::lock_guard<std::mutex> lock(pendingMutex);
                pendingOps.erase(op);
            }
            op->promise.set_value(res);
            delete op;
        }
        
        io_uring_cqe_seen(&ring, cqe);
    }
}
