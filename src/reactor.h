#ifndef REACTOR_H
#define REACTOR_H
#include<functional>
#include<sys/epoll.h>
#include<unistd.h>

//前置声明
class Connection;
//代替了 typedef void (*EventCallback)(Connection *c)
using EventCallback = std::function<void (Connection*)>; 

//前置声明
class Reactor;

class Connection
{
public:
    char pending[2048]; //缓存还未写完的数据
    int pending_len; //还剩多少未发送
    int pending_off; //从哪里开始发送
    EventCallback on_read;
    EventCallback on_write;

    Connection(int fd,Reactor *r):fd_(fd),reactor_(r){}

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


};

class Reactor
{
public:
    Reactor();
    ~Reactor(); //close(epfd_)
    void loop(); 
    void add_event(int fd,Connection *conn,u_int32_t events); //ADD + data.ptr 注册新 fd
    void mod_event(int fd,Connection *conn,u_int32_t events); //MOD + data.ptr 修改已注册 fd 在 epoll 里关注的事件




private:
    int epfd_;    
};

#endif