#ifndef REACTOR_H
#define REACTOR_H
#include "net/connection.h"
#include<sys/epoll.h>
#include<unistd.h>
#include<memory>
#include<unordered_map>
#include<vector>
#include<cerrno>
#include<iostream>


class Reactor
{
public:
    Reactor();
    ~Reactor(); //close(epfd_)

    Reactor(const Reactor&) = delete;
    Reactor& operator = (const Reactor&) = delete;

    Reactor(Reactor&&) = delete;
    Reactor& operator = (Reactor&&) = delete;




    bool loop(); 
    [[nodiscard]]bool add_event(int fd,Connection *conn,u_int32_t events); //ADD + data.ptr 注册新 fd
    [[nodiscard]]bool mod_event(int fd,Connection *conn,u_int32_t events); //MOD + data.ptr 修改已注册 fd 在 epoll 里关注的事件

    //接管连接所有权 + 注册事件
    void add_connection(int fd,std::unique_ptr<Connection> conn,uint32_t events);
    //申请关闭 实际关闭在 drain_pending_close()中
    void request_close(Connection*c);


private:
    int epfd_;    
    void drain_pending_close(); //事件处理完后统一销毁

    //储存所有 conn fd_
    std::unordered_map<int,std::unique_ptr<Connection>> connections_;

    //标记批次内申请关闭的 fd_
    std::vector<int> pending_close_;

};

#endif