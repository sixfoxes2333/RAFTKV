#ifndef CONNECTION_H
#define CONNECTION_H
#include "base/buffer.h"
#include "base/limits.h"
#include<functional>
#include<unistd.h>
#include<cstdint>
#include<sys/epoll.h>
#include<chrono>

//前置声明
class Connection;
//代替了 typedef void (*EventCallback)(Connection *c)
using EventCallback = std::function<void (Connection*)>; 

//连接默认关注的事件 : 可读 + 对端半关闭通知
const uint32_t CONN_BASE_EVENTS = EPOLLIN | EPOLLRDHUP;

//发送队列水位
const size_t OUT_HIGH_WATER = 2*1024*1024; //2MB 超过则暂停读
const size_t OUT_LOW_WATER = 512*1024; //512KB 降到 512 KB 才恢复读
const size_t OUT_HARD_CAP = kv::MAX_MESSAGE + 1024*1024; //17MB 超出则放弃连接


//前置声明
class Reactor;

class Connection
{
public:
    Buffer in_; //收
    Buffer out_; //发
    EventCallback on_read;
    EventCallback on_write;

    Connection(int fd,Reactor *r):fd_(fd),reactor_(r){}
    ~Connection(); //close(fd_) 唯一的关闭出口

    Connection(const Connection&) = delete;
    Connection& operator = (const Connection&) = delete;
    Connection(Connection&&) = delete;
    Connection& operator = (Connection&&) = delete;

    //标记申请关闭
    void mark_closing()
    {
        closing_ = true;
    }

    //返回申请关闭状态
    bool closing()const
    {
        return closing_;
    }

    //设置掩码
    void set_events(uint32_t events)
    {
        events_ = events;
    }

    //返回掩码
    uint32_t events()const
    {
        return events_;
    }

    //数据没发完，等待可写
    void enable_write();

    //数据发完
    void disable_write();

    //对端已半关闭，不再要可读通知
    void close_read();

    //标识对端关闭了写方向
    void mark_peer_closed()
    {
        peer_closed_ = true;
    }

    //返回标识
    bool peer_closed()const
    {
        return peer_closed_;
    }

    //在途请求 即已经提交给后台线程结果还没回来的请求个数
    void add_pending_request()
    {
        pending_requests_++;
    }

    void finish_pending_request()
    {
        if(pending_requests_ > 0)
        {
            pending_requests_--;
        }
    }

    size_t pending_requests()const
    {
        return pending_requests_;
    }

    void add_inflight_bytes(size_t n)
    {
        inflight_bytes_ += n;
    }

    void release_inflight_bytes(size_t n)
    {
        inflight_bytes_ = (inflight_bytes_ > n) ? (inflight_bytes_ - n) : 0;
    }


    size_t inflight_bytes()const
    {
        return inflight_bytes_;
    }


    //连接身份由 reactor 在 add_connection 时分配 单增 永不复用
    uint64_t connection_id()const
    {
        return connection_id_;
    }

    void set_connection_id(uint64_t id)
    {
        connection_id_ = id;
    }



    int fd() const
    {
        return fd_;
    }

    Reactor *reactor()
    {
        return reactor_;
    }


    //暂停读方向 保留 EPOLLRDHUP
    void pause_read()
    {
        if(read_paused_)
        {
            return;
        }

        read_paused_ = true;
        events_ = (events_ & ~EPOLLIN) | EPOLLRDHUP;
        apply_events(); 
    }

    //恢复读方向
    void resume_read()
    {
        if(!read_paused_)
        {
            return;
        }
        read_paused_ = false;
        events_ |= EPOLLIN | EPOLLRDHUP;
        apply_events();
    }

    bool read_paused()const
    {
        return read_paused_;
    }

    //收包缓冲区里是否还剩发了一半的帧
    bool has_partial_frame()const
    {
        return in_.readable() > 0;
    }

    //记录最近一次收到新字节的时刻，判断半帧是否卡住
    void touch_read_progress()
    {
        last_progress_ = std::chrono::steady_clock::now();
    }

    std::chrono::steady_clock::time_point last_progress()const
    {
        return last_progress_;
    }




private:
    int fd_;
    Reactor *reactor_;

    bool closing_ = false; //申请关闭标识

    uint32_t events_ = 0; //当前在 epoll 中关注的掩码

    bool peer_closed_ = false; //标识对端是否关闭了写方向 (半关闭)

    uint64_t connection_id_ = 0; //连接代数 0 = 未分配

    size_t pending_requests_ = 0; //在途请求数

    bool read_paused_ = false; //读方向是否因为发送队列水位被暂停

    void apply_events();

    size_t inflight_bytes_ = 0;

    std::chrono::steady_clock::time_point last_progress_ = std::chrono::steady_clock::now();

};

#endif