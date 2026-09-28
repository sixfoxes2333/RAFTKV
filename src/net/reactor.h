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
    [[nodiscard]]bool add_connection(int fd,std::unique_ptr<Connection> conn,uint32_t events);
    //申请关闭 实际关闭在 drain_pending_close()中
    void request_close(Connection*c);

    //按照 fd 与 连接代数 找回连接
    Connection* find_connection(int fd,uint64_t connection_id);


    //停止事件循环
    void stop();

    //收包缓冲 所有连接半帧占的字节之和
    void add_in_bytes(size_t n)
    {
        in_bytes_ += n;
    }

    void release_in_bytes(size_t n)
    {
        in_bytes_ = (in_bytes_ > n) ? (in_bytes_ - n) : 0;
    }

    size_t in_bytes()const
    {
        return in_bytes_;
    }

    //半帧超时
    void set_frame_timeout_ms(int ms)
    {
        frame_timeout_ms_ = ms;
    }


private:
    int epfd_;    
    void drain_pending_close(); //事件处理完后统一销毁

    //储存所有 conn fd_
    std::unordered_map<int,std::unique_ptr<Connection>> connections_;

    //标记批次内申请关闭的 fd_
    std::vector<int> pending_close_;

    uint64_t next_conn_id_ = 1; //连接代数分配器，从 1 开始， 0 保留给未分配

    bool running_ = false; //loop 的运行标志

    //扫描半帧卡住的连接
    void sweep_partial_frames();

    size_t in_bytes_ = 0; //所有 缓冲的字节数
    int frame_timeout_ms_ = 30000; //超时

};

#endif