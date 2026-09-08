#include"reactor.h"
#include<sys/socket.h>
#include<netinet/in.h>
#include<fcntl.h>
#include<cerrno>
#include<iostream>
#include<cstring>

//设置非阻塞模式
void setnonblocking(int fd)
{
    int old = fcntl(fd,F_GETFL);
    fcntl(fd,F_SETFL,old | O_NONBLOCK);
}

//短写续写
void handle_write(Connection* c)
{
    while(c->pending_len > 0)
    {   //write 返回成功写入的字节数
        int n = write(c->fd(),c->pending + c->pending_off,c->pending_len);
        if(n > 0)
        {
            c->pending_len -= n;
            c->pending_off += n;
        }
        else if(n == 1 && errno == EAGAIN)
        {
            //缓冲区满，等待 EPOLLOUT
            c->reactor()->mod_event(c->fd(),c,EPOLLOUT);
            return;
        }
        else
        {
            close(c->fd());
            delete c;
            return;
        }
    }

    c->reactor()->mod_event(c->fd(),c,EPOLLIN);
}

//读侧回调
void handle_read(Connection* c)
{
    char buf[2048];
    //read 返回实际读取字节数
    int len = read(c->fd(),buf,sizeof(buf));
    if(len > 0)
    {
        memcpy(c->pending,buf,len);
        c->pending_len = len;
        c->pending_off = 0;


        //立刻回写，以后再更改为收发
        handle_write(c);

    }
    else if(len == 0)
    {
        std::cout << "\n peer closed fd = " << c->fd() << " \n" <<std::endl;
        close(c->fd());
        delete c;  
    }
    else
    {
        if(errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return;
        }
        close(c->fd());
        delete c;
    }
}

//建档回调
void handle_accept(Connection *lc)
{
    struct sockaddr_in client;
    socklen_t len = sizeof(client);
    int connfd = accept(lc->fd(),(struct sockaddr*)&client,&len);
    if(connfd < 0)
    {
        return;
    }

    setnonblocking(connfd);

    // //测试注入 : 缩小发送缓冲，强制触发短写

    // int sndbuf = 4096;
    // setsockopt(connfd,SOL_SOCKET,SO_SNDBUF,&sndbuf,sizeof(sndbuf));

    // //测试完注释掉

    Connection *c = new Connection(connfd,lc->reactor());
    c->on_read = handle_read;
    c->on_write = handle_write;
    lc->reactor()->add_event(connfd,c,EPOLLIN);
    std::cout << "\n new conn fd = " << connfd << " \n" << std::endl;
}


int main()
{
    
    //监听  IPV4 协议 TCP 拿取到文件描述符
    int listenfd = socket(AF_INET,SOCK_STREAM,0);
    setnonblocking(listenfd);

    //具体设置 socket 选项 开启地址复用
    int opt = 1;
    setsockopt(listenfd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    //指定内存空间前 n 个字节清零
    struct sockaddr_in addr;
    memset(&addr,0,sizeof(addr));

    //设置套接字地址结构
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(9006);

    bind(listenfd,(struct sockaddr*)&addr,sizeof(addr));
    listen(listenfd,5);

    Reactor reactor;
    Connection *lc = new Connection(listenfd,&reactor);
    lc->on_read = handle_accept;
    lc->on_write = nullptr;
    reactor.add_event(listenfd,lc,EPOLLIN);

    reactor.loop();

    return 0;
}