#include "net/connection.h"
#include "net/reactor.h"


Connection::~Connection()
{
    close(fd_);
}

void Connection::enable_write()
{
    if(events_ & EPOLLOUT)
    {
        return; //已经设定为 EPOLLOUT 在等待了
    }

    events_ |= EPOLLOUT;

    apply_events();
}

void Connection::disable_write()
{
    //还没设定为 等待可写
    if(!(events_ & EPOLLOUT))
    {
        return;
    }

    events_ &= ~EPOLLOUT;

    apply_events();

}

void Connection::close_read()
{
    events_ &= ~(EPOLLIN | EPOLLRDHUP);
    
    apply_events();
}

void Connection::apply_events()
{
    if(!reactor_->mod_event(fd_,this,events_))
    {
        reactor_->request_close(this);
    }
}