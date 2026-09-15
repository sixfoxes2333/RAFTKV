#include "net/reactor.h"
#include<sys/socket.h>
#include<netinet/in.h>
#include<fcntl.h>
#include<cerrno>
#include<iostream>
#include<cstring>
#include<arpa/inet.h>
#include<memory>

const int MAX_MESSAGE = 16 * 1024 *1024; //防止恶意长度炸内存

//设置非阻塞模式
void setnonblocking(int fd)
{
    int old = fcntl(fd,F_GETFL);
    fcntl(fd,F_SETFL,old | O_NONBLOCK);
}

//短写续写
void handle_write(Connection* c)
{
    while(c->out_.readable() > 0)
    {   //send 返回成功写入的字节数
        //MSG_NOSIGNAL 对端已关闭时不产生 SIGPIPE (不然进程会被默认信号杀掉)
        ssize_t n = send(c->fd(),c->out_.peek(),c->out_.readable(),MSG_NOSIGNAL);
        if(n > 0)
        {
            c->out_.retrieve(n);
        }
        else if(n < 0 && errno == EINTR)
        {
            //被信号打断 什么都没写，重试即可
            continue;
        }
        else if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            //缓冲区满，等待 EPOLLOUT
            c->enable_write();
            return;
        }
        else if(n == 0) //理论上不会发生
        {
            return;
        }
        else
        {
            //真错误
            c->reactor()->request_close(c);
            return;
        }
    }

    if(c->peer_closed())
    {
        c->reactor()->request_close(c); //对端不会再发，则本体也不再发送
        return;
    }

    c->disable_write(); //掩码回到 EPOLLIN | EPOLLRDHUP

}

//处理消息
void handle_message(Connection *c , const char *payload , size_t len)
{
    uint32_t be = htonl(len);

    //echo 以后再改 SET/GET

    c->out_.append(reinterpret_cast<const char*>(&be),4); //追加长度头
    c->out_.append(payload,len);
    handle_write(c);
}


//检测切分
void process_frames(Connection *c)
{
    while(c->in_.readable() >= 4) //长度头就占四字节
    {
        uint32_t len;
        memcpy(&len,c->in_.peek(),4); //取前 4 字节
        len = ntohl(len); //大端转换为主机序

        //恶意长度
        if(len > MAX_MESSAGE)
        {
            std::cout << "bad len " << len << " from fd = " << c->fd() << std::endl;
            c->reactor()->request_close(c);
            return;
        }
        
        //半包 等待下次事件
        if(c->in_.readable() < 4 + len)
        {
            break;
        }

        //完整一条消息
        handle_message(c,c->in_.peek() + 4,len);

        //回调可能已申请关闭 (比如 发送失败) 
        if(c->closing())
        {
            return;
        }

        c->in_.retrieve(4+len);
        
    }
}



//读侧回调
void handle_read(Connection* c)
{
    char buf[2048];
    //read 返回实际读取字节数
    int len = read(c->fd(),buf,sizeof(buf));
    if(len > 0)
    {
        c->in_.append(buf,len);

        //检测切分
        process_frames(c);
        return;
    }
    else if(len == 0)
    {
        std::cout << "\n peer closed fd = " << c->fd() << " \n" <<std::endl; //对端半关闭，不再有新数据
        
        c->mark_peer_closed();

        //没有待发数据
        if(c->out_.readable() == 0)
        {
            c->reactor()->request_close(c);
        }
        else
        {
            //还有回复未发送
            //关掉读，再等 EPOLLOUT 把数据发完
            c->close_read();
            c->enable_write();
        }

        return;
        
    }
    else
    {
        if(errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return;
        }
        c->reactor()->request_close(c);
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

    auto c = std::make_unique<Connection>(connfd,lc->reactor());

    c->on_read = handle_read;
    c->on_write = handle_write;

    lc->reactor()->add_connection(connfd,std::move(c),CONN_BASE_EVENTS);
    
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

    auto lc = std::make_unique<Connection>(listenfd,&reactor);

    lc->on_read = handle_accept;
    lc->on_write = nullptr;
    
    reactor.add_connection(listenfd,std::move(lc),CONN_BASE_EVENTS);

    reactor.loop();

    return 0;
}