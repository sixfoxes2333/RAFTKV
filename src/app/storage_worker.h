#ifndef APP_STORAGE_WORKER_H
#define APP_STORAGE_WORKER_H

#include "net/protocol.h"
#include "storage/storage.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include<chrono>


namespace kv
{

    //从 Reator 线程送进后台线程一个任务
    struct StorageTask
    {
        int fd = -1; //用于回包 结果写回哪个 fd
        uint64_t connection_id = 0; //连接身份 fd 会被复用所以要保存以防止回错连接
        uint64_t sequence = 0; //投递序号
        Request request; //已解析好的协议请求

        bool parse_failed = false; 

        size_t bytes = 0; //该请求占用的字节
    };

    //后台线程执行完，从队列取回结果
    struct StorageCompletion
    {
        int fd = -1;
        uint64_t connection_id = 0;
        uint64_t sequence = 0;
        Response response; //要编码发回客户端的响应

        size_t bytes = 0; //原样带回
    };

    class StorageWorker
    {
    public:

        enum class SyncMode
        {
            Always, //每条都 fsync
            Everysec, //每秒 fsync
            Off //从不
        };


        //path 数据库文件 max_pending 队列上限
        explicit StorageWorker(const std::string & path,size_t max_pending = 1024,SyncMode mode = SyncMode::Always);
        ~StorageWorker();

        //不可复制或移动
        StorageWorker(const StorageWorker&) = delete;
        StorageWorker& operator=(const StorageWorker&) = delete;
        StorageWorker(StorageWorker&&) = delete;
        StorageWorker& operator=(StorageWorker&&) = delete;


        //构造成功为 true ，否则为该 worker 不可用
        bool ready()const
        {
            return ready_.load();
        }

        //完成通知用的 eventfd
        int wakeup_fd()const
        {
            return wakeup_fd_;
        }

        //Reactor 线程调用 只入队
        //false worker 不可用/正在停止/队列达上限
        bool submit(StorageTask task);

        //Reactor 线程在 eventfd 可读时调用
        std::vector<StorageCompletion> take_completed();



    private:    

        static constexpr size_t MAX_BATCH_SIZE = 256; //每批次最多攒多少条
        static constexpr size_t MAX_BATCH_BYTES = 8*1024*1024; //每批次最多请求多少字节

        bool flush_if_needed(bool force); //false 即落盘失败

        void worker_loop(); //后台线程入口
        Response execute(const Request &req); //存储调用
        void notify_locked(); //写 eventfd 调用要持有 mutex_

        Storage storage_;
        int wakeup_fd_ = -1;
        size_t max_pending_ = 0;

        size_t inflight_bytes_ = 0; //队列中所有任务的请求体总字节

        SyncMode mode_ = SyncMode::Always;
        bool dirty_ = false;
        std::chrono::steady_clock::time_point last_flush_ = std::chrono::steady_clock::now();


        std::atomic<bool>ready_{false};

        std::mutex mutex_;
        std::condition_variable cv_;
        std::deque<StorageTask> pending_;
        std::deque<StorageCompletion>completed_;
        bool stopping_ = false;
        std::thread worker_;


    };




}//namspace kv

#endif
