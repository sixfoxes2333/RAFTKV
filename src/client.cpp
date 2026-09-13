#include<sys/socket.h>
#include<iostream>
#include<cstring>
#include<cstdlib>
#include<string>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<unistd.h>


//将一块长度的内存数据包装为 TCP 数据帧并发送到 socket 缓冲区
void send_frame(int fd,const char* data,size_t len)
{
    uint32_t be = htonl(len);
    const char* p = reinterpret_cast<const char*>(&be); //len 转换为长度头


    size_t sent = 0;
    //while 循环处理短写
    while(sent < 4)
    {
        int n = write(fd,p + sent,4-sent);

        if(n <= 0)
        {
            perror("write");

            exit(1);
        }

        sent += n;
    }
    
    sent = 0;
    //while 循环处理短写
    while(sent < len)
    {
        int n = write(fd,data + sent,len - sent);
        
        if(n <= 0)
        {
            perror("write");
            exit(1);
        }
        
        sent += n;
    }
}

//读一条完整帧内容，对端断开返回 false
bool recv_frame(int fd,std::string &payload)
{
    //包头读取
    char header[4];
    size_t got = 0;
    while(got < 4) //读长度头
    {
        int n = read(fd,header + got,4-got);

        if(n == 0)
        {
            return false;
        }

        if(n < 0)
        {
            perror("read");
            return false;
        }

        got += n;
    }

    //取到要接收的数据的长度
    uint32_t len;
    memcpy(&len,header,4);
    len = ntohl(len); //大端转换为主机序
    payload.resize(len);


    got = 0;
    while(got < len)
    {
        int n = read(fd,&payload[0] + got,len-got);
        if(n == 0)
        {
            return false;
        }

        if(n < 0)
        {
            perror("read");
            return false;
        }

        got += n;
    }

    return true;

}


int main(int argc,char *argv[])
{
    //确保用户在启动程序时至少传入一个待发送的消息
    if(argc < 2)
    {
        std::cout << "usage: " << argv[0] << " msg1 msg2 ..." << std::endl;
        return 1;
    }

    int fd = socket(AF_INET,SOCK_STREAM,0);

    struct sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(9006);
    inet_pton(AF_INET,"127.0.0.1",&addr.sin_addr);
    connect(fd,(struct sockaddr*)&addr,sizeof(addr));

    //从 argv[1] 开始，因为 [0] 是程序名字
    for(int i = 1 ; i < argc ; i++)
    {
        send_frame(fd,argv[i],strlen(argv[i]));
    }

    shutdown(fd,SHUT_WR); //半关闭 只关闭写

    std::string reply;
    while(recv_frame(fd,reply))
    {
        std::cout << "reply: " << reply << std::endl;
    }

    close(fd);
    return 0;


}