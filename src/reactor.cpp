#include"reactor.h"

Reactor::Reactor():epfd_(-1) //构造对象时将 epfd_ 初始化为 -1
{
    epfd_ = epoll_create(5);
}

Reactor::~Reactor()
{
    close(epfd_);
}

void Reactor::loop()
{
    
}

void Reactor::add_event(int fd,Connection *conn,uint32_t events)
{
    struct epoll_event ev;
    ev.events = events;
    ev.data.ptr = conn;
    epoll_ctl(epfd_,EPOLL_CTL_ADD,fd,&ev);
}

void Reactor::mod_event(int fd,Connection *conn,uint32_t events)
{
    struct epoll_event ev;
    ev.events = events;
    ev.data.ptr = conn;
    epoll_ctl(epfd_,EPOLL_CTL_MOD,fd,&ev);
}
