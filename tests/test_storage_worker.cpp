// test_storage_worker.cpp —— 后台存储线程 + eventfd 通知测试（M6b）
//
// 【要验证的架构】
//   Reactor 线程只 submit，不碰 Storage、不等待 fsync；StorageWorker 的唯一后台线程
//   按 FIFO 执行 put/get/del，把结果放入完成队列，再 write(eventfd) 唤醒调用方。
//
// 【不依赖 Reactor】
//   这个测试直接 poll(wakeup_fd)，再 take_completed()，所以能独立验证 worker：
//     1) SET/GET/覆盖/DEL/NOT_FOUND 的语义
//     2) 单线程 FIFO：SET(a,1) → SET(a,2) → GET(a) 必须拿到 2
//     3) fd / connection_id / sequence 原样带回（M6c 用它防 fd 复用后回错连接）
//     4) eventfd 被 take_completed() 读空，不产生忙循环
//     5) worker 析构会 drain 已排队任务并 join（不丢已接收任务）
//     6) 重建 worker 后能读到持久化数据（Storage::recover 真正在异步路径生效）
//     7) max_pending=0 时 submit 被拒绝（过载保护）
//
// 【当前预期】
//   src/app/storage_worker.{h,cpp} 尚未实现 → 构建为红；实现后全绿。
#include "app/storage_worker.h"
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <poll.h>
#include <string>
#include <utility>
#include <vector>

static int g_failed = 0;

#define CHECK(cond)                                                             \
    do {                                                                        \
        if (cond)                                                               \
            std::cout << "  ok   : " #cond << std::endl;                        \
        else {                                                                  \
            std::cout << "  FAIL : " #cond "   (line " << __LINE__ << ")"       \
                      << std::endl;                                              \
            g_failed++;                                                         \
        }                                                                       \
    } while (0)

static kv::StorageTask make_task(int fd,uint64_t conn_id,uint64_t seq,
                                 kv::Op op,std::string key,std::string value = "")
{
    kv::StorageTask task;
    task.fd = fd;
    task.connection_id = conn_id;
    task.sequence = seq;
    task.request.op = op;
    task.request.key = std::move(key);
    task.request.value = std::move(value);
    return task;
}

static std::vector<kv::StorageCompletion> wait_completed(kv::StorageWorker &worker,
                                                         size_t wanted,
                                                         int timeout_ms = 10000)
{
    std::vector<kv::StorageCompletion> out;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

    while(out.size() < wanted)
    {
        auto now = std::chrono::steady_clock::now();
        if(now >= deadline)
        {
            break;
        }

        int remain = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());

        struct pollfd pfd;
        pfd.fd = worker.wakeup_fd();
        pfd.events = POLLIN;
        pfd.revents = 0;

        int rc;
        do
        {
            rc = poll(&pfd,1,remain);
        }
        while(rc < 0 && errno == EINTR);

        if(rc <= 0)
        {
            break;
        }

        auto batch = worker.take_completed();
        for(auto &done : batch)
        {
            out.push_back(std::move(done));
        }
    }

    return out;
}

static bool eventfd_is_drained(int fd)
{
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    return poll(&pfd,1,0) == 0;
}

int main()
{
    const char *PATH = "/tmp/kv_worker_test.log";
    std::remove(PATH);

    std::cout << "[1] FIFO semantics + identity preservation + eventfd" << std::endl;
    {
        kv::StorageWorker worker(PATH);
        CHECK(worker.ready());
        CHECK(worker.wakeup_fd() >= 0);

        const int fd = 42;
        const uint64_t conn_id = 1001;

        CHECK(worker.submit(make_task(fd,conn_id,1,kv::Op::Set,"a","1")));
        CHECK(worker.submit(make_task(fd,conn_id,2,kv::Op::Get,"a")));
        CHECK(worker.submit(make_task(fd,conn_id,3,kv::Op::Set,"a","2")));
        CHECK(worker.submit(make_task(fd,conn_id,4,kv::Op::Get,"a")));
        CHECK(worker.submit(make_task(fd,conn_id,5,kv::Op::Del,"a")));
        CHECK(worker.submit(make_task(fd,conn_id,6,kv::Op::Get,"a")));
        CHECK(worker.submit(make_task(fd,conn_id,7,kv::Op::Get,"missing")));
        CHECK(worker.submit(make_task(fd,conn_id,8,kv::Op::Set,"persist","saved")));

        auto done = wait_completed(worker,8);
        CHECK(done.size() == 8);

        if(done.size() == 8)
        {
            for(size_t i = 0; i < done.size(); i++)
            {
                CHECK(done[i].fd == fd);
                CHECK(done[i].connection_id == conn_id);
                CHECK(done[i].sequence == i + 1);       //单线程 FIFO → 完成顺序必须等于投递顺序
            }

            CHECK(done[0].response.status == kv::Status::Ok);         //SET a=1
            CHECK(done[1].response.status == kv::Status::Ok);         //GET a
            CHECK(done[1].response.value == "1");
            CHECK(done[2].response.status == kv::Status::Ok);         //SET a=2
            CHECK(done[3].response.status == kv::Status::Ok);         //GET a
            CHECK(done[3].response.value == "2");                    //必须看到前一个 SET
            CHECK(done[4].response.status == kv::Status::Ok);         //DEL a
            CHECK(done[5].response.status == kv::Status::NotFound);   //GET a after DEL
            CHECK(done[6].response.status == kv::Status::NotFound);   //GET missing
            CHECK(done[7].response.status == kv::Status::Ok);         //SET persist=saved
        }

        CHECK(eventfd_is_drained(worker.wakeup_fd())); //take_completed 必须把 eventfd 读空
    }

    std::cout << "[2] persistence after worker recreation" << std::endl;
    {
        kv::StorageWorker worker(PATH);
        CHECK(worker.ready());
        CHECK(worker.submit(make_task(7,700,9,kv::Op::Get,"persist")));
        CHECK(worker.submit(make_task(7,700,10,kv::Op::Get,"a")));

        auto done = wait_completed(worker,2);
        CHECK(done.size() == 2);
        if(done.size() == 2)
        {
            CHECK(done[0].response.status == kv::Status::Ok);
            CHECK(done[0].response.value == "saved");
            CHECK(done[1].response.status == kv::Status::NotFound);   //墓碑跨重启仍生效
        }
    }

    std::cout << "[3] max_pending overload protection" << std::endl;
    const char *PATH_CAP = "/tmp/kv_worker_cap.log";
    std::remove(PATH_CAP);
    {
        kv::StorageWorker worker(PATH_CAP,0);  //容量 0：任何 submit 都必须立刻拒绝
        CHECK(worker.ready());
        CHECK(!worker.submit(make_task(1,1,1,kv::Op::Set,"k","v")));
    }

    std::cout << "[4] destructor drains queued tasks before join" << std::endl;
    const char *PATH_DRAIN = "/tmp/kv_worker_drain.log";
    std::remove(PATH_DRAIN);
    {
        kv::StorageWorker worker(PATH_DRAIN);
        CHECK(worker.ready());

        for(int i = 0; i < 30; i++)
        {
            CHECK(worker.submit(make_task(9,900,static_cast<uint64_t>(i),
                                          kv::Op::Set,"d" + std::to_string(i),
                                          "v" + std::to_string(i))));
        }
        //故意不 take_completed：离开作用域时析构函数必须等后台线程把 30 个任务处理完
    }
    {
        kv::StorageWorker worker(PATH_DRAIN);
        CHECK(worker.ready());
        CHECK(worker.submit(make_task(10,1000,100,kv::Op::Get,"d29")));

        auto done = wait_completed(worker,1);
        CHECK(done.size() == 1);
        if(done.size() == 1)
        {
            CHECK(done[0].response.status == kv::Status::Ok);
            CHECK(done[0].response.value == "v29");
        }
    }

    if(g_failed == 0)
    {
        std::cout << "\nall storage worker tests passed" << std::endl;
        return 0;
    }

    std::cout << "\n" << g_failed << " storage worker check(s) FAILED" << std::endl;
    return 1;
}
