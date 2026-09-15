#ifndef REACTOR_H
#define REACTOR_H
#include "net/connection.h"
#include<sys/epoll.h>
#include<unistd.h>

//前置声明
class Reactor;

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