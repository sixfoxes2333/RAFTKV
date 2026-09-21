#include "app/storage_worker.h"
#include <cerrno>
#include <cstring>
#include <exception>
#include <iostream>
#include <sys/eventfd.h>
#include <unistd.h>
#include <utility>

namespace kv
{

    StorageWorker::StorageWorker(const std::string &path,size_t max_pending):storage_(path),max_pending_(max_pending)
    {
        //打开文件并 recover()
        //失败则 good() 为 false
        if(!storage_.good())
        {
            std::cerr << "StorageWorker: storage open/recover failed" << std::endl;
            return; //ready_ 保持 false
        }

        //eventfd 通知有无结果可取
        //EFD_NONBLOCK:Reactor 在没结果时 read 应该是 EAGAIN
        //EFD_CLOEXEC exec 时自动关闭
        wakeup_fd_ = eventfd(0,EFD_NONBLOCK | EFD_CLOEXEC);
        if(wakeup_fd_ < 0)
        {
            std::cerr << "eventfd failed : " << strerror(errno) << std::endl;
            return;
        }

        //启动后台线程
        try
        {
            worker_ = std::thread(&StorageWorker::worker_loop,this);
            ready_.store(true);
        }
        catch(const std::exception &e)
        {
            std::cerr << "Storage thread create failed : " << e.what() << std::endl;

            close(wakeup_fd_);
            wakeup_fd_ = -1;
        }

    }

    StorageWorker::~StorageWorker()
    {
        //不再收新的任务 并 加锁
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }

        //唤醒可能沉睡的 cv.wait 里的 worker
        cv_.notify_one();

        //将接收的 pending_ 全部做完再退出
        if(worker_.joinable())
        {
            worker_.join();
        }

        //线程停后关闭 eventfd
        ready_.store(false);

        if(wakeup_fd_ >= 0)
        {
            close(wakeup_fd_);
            wakeup_fd_ = -1;
        }

    }


    bool StorageWorker::submit(StorageTask task)
    {
        //锁外快速拒绝
        if(!ready_.load())
        {
            return false;
        }

        {
            std::lock_guard<std::mutex>lock(mutex_);

            //锁内再次确认 stopping_;
            if(stopping_ || !ready_.load())
            {
                return false;
            }

            //过载保护 : 任务队列和完成队列都占内存
            //至多一个任务在 worker 手里执行，它不在任何队列里
            if(pending_.size() + completed_.size() >= max_pending_)
            {
                return false;
            }

            pending_.push_back(std::move(task));

        }

        //通知放在锁外
        cv_.notify_one();
        return true;

    }

    void StorageWorker::worker_loop()
    {
        while(true)
        {
            StorageTask task;

            {
                std::unique_lock<std::mutex>lock(mutex_);

                //直到有新任务才被唤醒
                cv_.wait(lock,[this]{return stopping_ || !pending_.empty();});

                if(stopping_ && pending_.empty())
                {
                    break;
                }

                task = std::move(pending_.front()); //从头部取
                pending_.pop_front();

            }

            StorageCompletion done;
            done.fd = task.fd;
            done.connection_id = task.connection_id;
            done.sequence = task.sequence;
            done.response = execute(task.request);

            {
                std::lock_guard<std::mutex>lock(mutex_);
                completed_.push_back(std::move(done));
                notify_locked();
            }
        }
    }


    Response StorageWorker::execute(const Request &req)
    {
        Response resp; //默认构造出来就是 Status::Error : 默认走失败路径 (fail-closed)

        //Storage 已经不可用
        if(!storage_.good())
        {
            ready_.store(false);
            return resp;
        }

        switch(req.op)
        {
            case Op::Set:
            {    //put 内部 编码->追加写->fsync->更新 keydir
                //false = 写失败 或 提交结果不确定

                resp.status = storage_.put(req.key,req.value) ? Status::Ok : Status::Error;
                break;
            }

            case Op::Get:
            {
                //先查内存索引 不存在则 NotFound
                if(!storage_.contains(req.key))
                {
                    resp.status = Status::NotFound;
                    break;
                }

                std::string value;
                if(storage_.get(req.key,value))
                {
                    resp.status = Status::Ok;
                    resp.value = std::move(value);
                }
                else
                {
                    resp.status = Status::Error;
                }

                break;
            }

            case Op::Del:
            {
                if(!storage_.contains(req.key))
                {
                    resp.status = Status::NotFound;
                }
                else
                {
                    resp.status = storage_.del(req.key) ? Status::Ok : Status::Error;
                }
                break;
            }
        }

        if(!storage_.good())
        {
            ready_.store(false);
        }

        return resp;
    }

    void StorageWorker::notify_locked()
    {
        //调用方必须持有 mutex_
        uint64_t one = 1;
        ssize_t n;

        do
        {
            n = write(wakeup_fd_,&one,sizeof(one));
        }
        while(n < 0 && errno == EINTR); //EINTR 重试

        if(n == static_cast<ssize_t>(sizeof(one)))
        {
            return;//通知成功
        }

        //EAGAIN = 计数器满
        if(n < 0 && errno == EAGAIN)
        {
            return;
        }

        //真的写失败了
        std::cerr << "eventfd write failed : " << strerror(errno) << std::endl;
        ready_.store(false);

    }

    std::vector<StorageCompletion> StorageWorker::take_completed()
    {
        std::vector<StorageCompletion> out;

        if(wakeup_fd_ < 0)
        {
            return out;
        }

        std::lock_guard<std::mutex>lock(mutex_);

        uint64_t counter = 0;
        ssize_t n;

        do
        {
            n = read(wakeup_fd_,&counter,sizeof(counter));
        }
        while(n < 0 && errno == EINTR);

        if(n < 0 && errno != EAGAIN)
        {
            std::cerr << "eventfd read failed : " << strerror(errno) << std::endl;
            ready_.store(false);
        }

        //一次性搬空，减少后序唤醒次数
        out.reserve(completed_.size());
        while(!completed_.empty())
        {
            out.push_back(std::move(completed_.front()));
            completed_.pop_front();
        }

        return out;
    }





}//namespace kv