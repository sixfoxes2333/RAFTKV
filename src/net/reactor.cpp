#include "net/reactor.h"
#include<chrono>

Reactor::Reactor():epfd_(-1) //构造对象时将 epfd_ 初始化为 -1
{
    epfd_ = epoll_create(5);
}

Reactor::~Reactor()
{
    close(epfd_);
}

bool Reactor::loop()
{
    struct epoll_event events[1024];
    running_ = true;
    while(running_)
    {
        //epoll_wait 返回准备好进行 I/O 操作 fd 的数量
        int n = epoll_wait(epfd_,events,1024,1000);

        if(n < 0)
        {
            if(errno == EINTR)
            {
                continue;
            }

            std::cerr<< "epoll_wait failed : " << strerror(errno) << std::endl;
            return false;

        }
    
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

        sweep_partial_frames();

        drain_pending_close();

    }

    return true;


}

bool Reactor::add_event(int fd,Connection *conn,uint32_t events)
{
    struct epoll_event ev;
    ev.events = events;
    ev.data.ptr = conn;

    if(epoll_ctl(epfd_,EPOLL_CTL_ADD,fd,&ev) < 0)
    {
        std::cerr << "epoll_ctl ADD fd = " << fd << " failed : " << strerror(errno) << std::endl;
        return false;
    }

    return true;
}

bool Reactor::mod_event(int fd,Connection *conn,uint32_t events)
{
    struct epoll_event ev;
    ev.events = events;
    ev.data.ptr = conn;

    if(epoll_ctl(epfd_,EPOLL_CTL_MOD,fd,&ev) < 0)
    {
        std::cerr << "epoll_ctl MOD fd = " << fd << " failed : " << strerror(errno) << std::endl;
        return false;
    }

    return true;
}


bool Reactor::add_connection(int fd,std::unique_ptr<Connection>conn,u_int32_t events)
{

    conn->set_connection_id(next_conn_id_++);//分配连接代数
    
    conn->set_events(events); // Connection 的记录与注册值保持一致
    if(!add_event(fd,conn.get(),events)) //注册
    {
        return false;
    }

    connections_[fd] = std::move(conn); //所有权进入 map
    return true;
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

        auto it = connections_.find(fd);

        if(it != connections_.end())
        {
            release_in_bytes(it->second->in_.readable());

            //fd 关闭时内核自动将其从 epoll 去掉 不再需要 EPOLL_CTL_DEL
            connections_.erase(fd);
        }
    }

    pending_close_.clear();
}

Connection* Reactor::find_connection(int fd,uint64_t connection_id)
{
    auto it = connections_.find(fd);

    if(it == connections_.end())
    {
        return nullptr; //连接已经关闭并销毁
    }

    if(it->second->connection_id() != connection_id)
    {
        return nullptr; //同一 fd 号，但是已经是另一连接
    }

    return it->second.get();
}


void Reactor::stop()
{
    running_ = false;
}

void Reactor::sweep_partial_frames()
{
    if(frame_timeout_ms_ <= 0)
    {
        return;
    }

    auto now = std::chrono::steady_clock::now();

    for(auto &kv : connections_)
    {
        Connection* c = kv.second.get();

        if(c->closing() || !c->has_partial_frame())
        {
            continue;
        }

        long long idle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - c->last_progress()).count();

        if(idle_ms > frame_timeout_ms_)
        {
            std::cerr << "partial frame idle " << idle_ms << "ms,close fd  = " << c->fd() << std::endl;
            request_close(c);
        }
    }

}