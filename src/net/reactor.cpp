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
    while(true)
    {
        //epoll_wait 返回准备好进行 I/O 操作 fd 的数量
        int n = epoll_wait(epfd_,events,1024,-1);
    
        for(int i = 0 ; i < n ; i++)
        {
            Connection *c = static_cast<Connection *>(events[i].data.ptr);
            uint32_t ev = events[i].events;

            if(c->closing())
            {
                continue;
            }

            if(ev & (EPOLLERR | EPOLLHUP))
            {
                request_close(c);
                continue;
            }
            
            if(ev & (EPOLLIN | EPOLLRDHUP)&& c->on_read)
            {
                c->on_read(c);
            }
            
            if(!c->closing() && (ev & EPOLLOUT) && c->on_write)
            {
                c->on_write(c);
            }
        }

        drain_pending_close();

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


void Reactor::add_connection(int fd,std::unique_ptr<Connection>conn,u_int32_t events)
{
    conn->set_events(events); // Connection 的记录与注册值保持一致
    add_event(fd,conn.get(),events); //注册
    connections_[fd] = std::move(conn); //所有权进入 map
}

void Reactor::request_close(Connection* c)
{
    if(c == nullptr || c->closing())
    {
        return; //重复申请直接返回
    }

    c->mark_closing();
    pending_close_.push_back(c->fd());
}

void Reactor::drain_pending_close()
{
    for(int fd : pending_close_)
    {
        //fd 关闭时内核自动将其从 epoll 去掉 不再需要 EPOLL_CTL_DEL
        connections_.erase(fd);
    }

    pending_close_.clear();
}