#include "net/protocol.h"
#include "base/limits.h"
#include<cstdint>
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
        ssize_t n = write(fd,p + sent,4-sent);

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
        ssize_t n = write(fd,data + sent,len - sent);
        
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

    if(len > kv::MAX_MESSAGE)
    {
        std::cerr << "frame too large : " << len << " > " << kv::MAX_MESSAGE << std::endl;
        return false;
    }


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
    if(argc < 3)
    {
        std::cerr << "usage : " << argv[0] << "set <key> <value>\n"
                  << "        " << argv[0] << "get <key>\n"
                  << "        " << argv[0] << "del <key>" << std::endl;

        return 2;
        
    }

    std::string cmd = argv[1];

    kv::Request req;

    if(cmd == "set")
    {
        if(argc < 4)
        {
            std::cerr << "usage : " << argv[0] << " set <key> <value>" << std::endl;
            return 2;
        }

        req.op = kv::Op::Set;
        req.key = argv[2];
        req.value = argv[3];
    }
    else if(cmd == "get")
    {
        req.op = kv::Op::Get;
        req.key = argv[2];
    }
    else if(cmd == "del")
    {
        req.op = kv::Op::Del;
        req.key = argv[2];
    }
    else
    {
        std::cerr << "unknow command : " << cmd << std::endl;
        return 2;
    }


    if(req.key.size() > kv::MAX_KEY || req.value.size() > kv::MAX_VALUE)
    {
        std::cerr << "key/value too large : key = " << req.key.size() << " (max " << kv::MAX_KEY << " ) ,value = " << req.value.size() << " (max " << kv::MAX_VALUE << " ) " << std::endl;
        return 2; 
    }


    //连接参数 : 默认回环 + 9006
    int port = 9006;

    if(const char* p = getenv("KV_PORT"))
    {
        port = atoi(p);
    }

    const char* host = getenv("KV_HOST");

    int fd = socket(AF_INET,SOCK_STREAM,0);

    if(fd < 0)
    {
        perror("socket");
        return 2;
    }

    struct sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET,host ? host : "127.0.0.1",&addr.sin_addr);

    if(connect(fd,(struct sockaddr*)&addr,sizeof(addr)) < 0)
    {
        perror("connect");
        close(fd);
        return 2;
    }

    std::string bytes = kv::encode_request(req);

    send_frame(fd,bytes.data(),bytes.size());

    shutdown(fd,SHUT_WR);

    std::string payload;

    if(!recv_frame(fd,payload))
    {
        std::cerr << "no response from server" << std::endl;
        close(fd);
        return 2;
    }

    kv::Response resp;

    if(!kv::parse_response(payload.data(),payload.size(),resp))
    {
        std::cerr << "malformed response" << std::endl;
        close(fd);
        return 2;
    }

    int rc = 2;

    switch(resp.status)
    {
        case kv::Status::Ok:
        {
            if(req.op == kv::Op::Get)
            {
                std::cout << resp.value;
            }
            else
            {
                std::cerr << "OK" << std::endl;
            }

            rc = 0;
            break;
        }

        case kv::Status::NotFound:
        {
            std::cerr << "NOT_FOUND" << std::endl;
            rc = 1;
            break;
        }

        case kv::Status::Error:
        {
            std::cerr << "ERROR" << std::endl;
            rc = 2;
            break;
        }
    }

    close(fd);

    return rc;

}