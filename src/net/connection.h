#ifndef CONNECTION_H
#define CONNECTION_H
#include "base/buffer.h"
#include<functional>
#include<unistd.h>
#include<cstdint>
#include<sys/epoll.h>

//前置声明
class Connection;
//代替了 typedef void (*EventCallback)(Connection *c)
using EventCallback = std::function<void (Connection*)>; 

//连接默认关注的事件 : 可读 + 对端半关闭通知
const uint32_t CONN_BASE_EVENTS = EPOLLIN | EPOLLRDHUP;


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



    int fd() const
    {
        return fd_;
    }

    Reactor *reactor()
    {
        return reactor_;
    }

private:
    int fd_;
    Reactor *reactor_;

    bool closing_ = false; //申请关闭标识

    uint32_t events_ = 0; //当前在 epoll 中关注的掩码

    bool peer_closed_ = false; //标识对端是否关闭了写方向 (半关闭)

};

#endif