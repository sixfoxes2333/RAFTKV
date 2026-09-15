#ifndef CONNECTION_H
#define CONNECTION_H
#include "base/buffer.h"
#include<functional>
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
    Buffer in_; //收
    Buffer out_; //发
    EventCallback on_read;
    EventCallback on_write;

    Connection(int fd,Reactor *r):fd_(fd),reactor_(r){}
    ~Connection(); //close(fd_) 唯一的关闭出口

    void mark_closing()
    {
        closing_ = true;
    }

    bool closing()const
    {
        return closing_;
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

};

#endif