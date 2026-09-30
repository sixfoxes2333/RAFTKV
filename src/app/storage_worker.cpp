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

    StorageWorker::StorageWorker(const std::string &path,size_t max_pending,SyncMode mode):storage_(path),max_pending_(max_pending),mode_(mode)
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

            //字节上限
            if(inflight_bytes_ + task.bytes > MAX_QUEUE_BYTES)
            {
                return false;
            }

            inflight_bytes_ += task.bytes;
            pending_.push_back(std::move(task));

        }

        //通知放在锁外
        cv_.notify_one();
        return true;

    }

    bool StorageWorker::flush_if_needed(bool force)
    {
        if(!dirty_)
        {
            return true;
        }

        if(!force)
        {
            if(mode_ == SyncMode::Everysec && std::chrono::steady_clock::now() - last_flush_ < std::chrono::seconds(1))
            {
                return true;
            }

            if(mode_ == SyncMode::Off)
            {
                return true;
            }
        }

        if(!storage_.flush())
        {
            return false;
        }

        dirty_ = false;
        last_flush_ = std::chrono::steady_clock::now();
        return true;


    }



    void StorageWorker::worker_loop()
    {

        std::vector<StorageTask>batch;
        std::vector<StorageCompletion>done_batch;
        std::vector<unsigned char>is_write;

        while(true)
        {
            batch.clear();
            {
                std::unique_lock<std::mutex>lock(mutex_);

                if(mode_ == SyncMode::Everysec)
                {
                    cv_.wait_for(lock,std::chrono::milliseconds(1000),[this]{return stopping_ || !pending_.empty();});
                }
                else
                {
                    cv_.wait(lock,[this]{return stopping_ || !pending_.empty();});
                }

                if(stopping_ && pending_.empty())
                {
                    lock.unlock();
                    flush_if_needed(true); //推出前落盘
                    break;
                }

                if(pending_.empty())
                {
                    lock.unlock();
                    flush_if_needed(false); //everysec 超时醒来落盘
                    continue;
                }

                size_t batch_bytes = 0;
                while(!pending_.empty() && batch.size() < MAX_BATCH_SIZE)
                {
                    if(!batch.empty() && batch_bytes + pending_.front().bytes > MAX_BATCH_BYTES)
                    {
                        break;
                    }

                    batch_bytes += pending_.front().bytes;
                    batch.push_back(std::move(pending_.front()));
                    pending_.pop_front();
                }
            }

            done_batch.clear();
            is_write.clear();
            bool any_write = false;

            for(auto &task : batch)
            {
                StorageCompletion done;
                done.fd = task.fd;
                done.connection_id = task.connection_id;
                done.sequence = task.sequence;
                done.bytes = task.bytes;

                bool write = false;
                if(task.parse_failed)
                {
                    done.response.status = Status::Error;
                }
                else
                {
                    done.response = execute(task.request);
                    write = (task.request.op != Op::Get);
                }

                any_write = any_write || write;
                is_write.push_back(write ? 1:0);
                done_batch.push_back(std::move(done));

            }

            if(any_write)
            {
                dirty_ = true;
            }

            if(!flush_if_needed(false))
            {
                for(size_t i = 0 ; i < done_batch.size() ; i++)
                {
                    if(is_write[i])
                    {
                        done_batch[i].response.status = Status::Error;
                    }
                }
            }

            {
                std::lock_guard<std::mutex>lock(mutex_);
                for(auto &done : done_batch)
                {
                    completed_.push_back(std::move(done));
                    notify_locked();
                }
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

                resp.status = storage_.put_deferred(req.key,req.value) ? Status::Ok : Status::Error;
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
                    resp.status = storage_.del_deferred(req.key) ? Status::Ok : Status::Error;
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
            //归还字节预算，只有把完成结果交给 reactor 才算真正离开队列
            if(inflight_bytes_ >= completed_.front().bytes)
            {
                inflight_bytes_ -= completed_.front().bytes;
            }
            else
            {
                inflight_bytes_ = 0;
            }

            out.push_back(std::move(completed_.front()));
            completed_.pop_front();
        }

        return out;
    }





}//namespace kv