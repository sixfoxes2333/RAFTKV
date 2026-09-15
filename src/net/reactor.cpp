#include "net/reactor.h"

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
    struct epoll_event events[1024];
    //epoll_wait 返回准备好进行 I/O 操作 fd 的数量
    while(true)
    {
        int n = epoll_wait(epfd_,events,1024,-1);
    

        for(int i = 0 ; i < n ; i++)
        {
            Connection *c = static_cast<Connection *>(events[i].data.ptr);
            uint32_t ev = events[i].events;

            if(ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
            {
                close(c->fd());
                delete c;
            }
            else if(ev & EPOLLIN && c->on_read)
            {
                c->on_read(c);
            }
            else if(ev & EPOLLOUT && c->on_write)
            {
                c->on_write(c);
            }
        }

    }


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
