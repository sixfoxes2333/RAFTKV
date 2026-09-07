#ifndef REACTOR_H
#define REACTOR_H
#include<functional>

//前置声明
class Connection;
//代替了 typedef void (*EventCallback)(Connection *c)
using EventCallback = std::function<void (Connection*)>; 

class Connection
{
public:
    int fd;
    char pending[2048]; //缓存还未写完的数据
    int pending_len; //还剩多少未发送
    int pending_off; //从哪里开始发送
    EventCallback on_read;
    EventCallback on_write;
};

class Reactor
{
public:
    Reactor();
    ~Reactor();
    void loop();





private:
    int epfd_;    
};

#endif