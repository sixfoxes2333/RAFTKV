#include"storage.h"
#include<cstring>
#include<arpa/inet.h>
#include<fcntl.h>
#include<unistd.h>
#include<cerrno>
#include<cstring>
#include<iostream>


static const char MAGIC[4] = {'K','V','D','B'}; //magic
static const uint32_t FORMAT_VERSION = 1; //版本号
static const size_t FILE_HEADER_SIZE = sizeof(MAGIC) + sizeof(FORMAT_VERSION); //文件头 magic + 版本号 = 8 字节


//将 buf 的 len 字节全部写出 PoSIX 原生 write 存在短写与信号打断问题
static bool write_all(int fd,const char* buf,size_t len)
{
    size_t done = 0;
    while(done < len)
    {
        ssize_t n = write(fd,buf+done,len-done);
        if(n < 0)
        {
            //如果被信号打断
            if(errno == EINTR)
            {
                continue;
            }

            //否则为真错误
            return false;
        }

        done += static_cast<size_t>(n);
    }

    return true;
}


//从 offset 处读满 len 字节 保证从磁盘读取时不丢失，不发生多线程冲突
static bool read_exact(int fd,char* buf,size_t len,off_t offset)
{
    size_t done = 0;
    while(done < len)
    {
        //如果用 read ，多个线程同时对同一个 WAL 文件读取，互相篡改文件指针位置，会导致乱套
        //除非使用互斥锁，但是并发性能就会下降
        //使用 pread ，读取时完全不改变文件的全局偏移指针，同时并发读取文件，实现无锁竞争的高并发读取
        ssize_t n = pread(fd,buf+done,len-done,offset + static_cast<off_t>(done));

        if(n < 0)
        {   
            //被信号中断
            if(errno == EINTR)
            {
                continue;
            }

            //真错误
            return false;
        }

        //如果文件提前结束 EOF 拦截
        if(n == 0)
        {
            return false;
        }

        done += static_cast<size_t>(n);
    }

    return true;

}



namespace kv
{

    //将 32 位无符号整数按照 大端字节序 转换为 4 个字节
    // 32 位无符号整数存的是 CRC key_len value_len
    static void append_be32(std::string &s,uint32_t v)
    {
        uint32_t be = htonl(v); //主机序转换为大端序
        s.append(reinterpret_cast<const char *>(&be),4); //将 4 字节追加写入缓冲区
    }

    //将 4 个字节转换为 32 位无符号整数
    static uint32_t read_be32(const char *p)
    {
        uint32_t be;
        memcpy(&be,p,4); //原样取得 4 字节
        return ntohl(be); //由
    }

    //查表加速 通过空间换时间
    uint32_t crc32(const char *data,size_t len)
    {
        static uint32_t table[256]; //生成 256 字节查找表，处理每字节仅需一次异或+查表
        static bool init = false; //仅在第一次调用时建表

        if(!init)
        {
            for(uint32_t i = 0 ; i < 256 ; i++)
            {
                uint32_t c = i;
                for(int k = 0 ; k < 8 ; k++)
                {
                    c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : (c >> 1); //逐位除法
                    table[i] = c;
                }
            }
            init = true;
        }

        uint32_t crc = 0xFFFFFFFFu; //初始化全为 1
        //逐字节滑动查表
        for(size_t i = 0 ; i < len ; i++)
        {
            crc = table[(crc ^ static_cast<unsigned char>(data[i])) & 0xFF] ^ (crc >> 8);
        }

        return crc ^ 0xFFFFFFFFu;
    }

    std::string encode_record(RecordType type,const std::string &key,const std::string &value)
    {
        std::string body;
        body.push_back(static_cast<char>(type)); //type 1 字节
        append_be32(body,key.size()); //key_len 大端 4 字节
        append_be32(body,value.size()); //val_len 大端 4 字节
        body += key; //key 原始字节
        body += value; //value 原始字节

        uint32_t c = crc32(body.data(),body.size()); //对 body 算 CRC

        //CRC 在前，body 在后
        std::string out;
        append_be32(out,c);
        out += body;

        return out;
    }


    bool decode_record(const char* buf,size_t len,Record &out,size_t &consumed)
    {
        //最小记录头 : CRC(4) + type(1) + key_len(4) + val_len(4) = 13

        if(len < 13)
        {
            return false;
        }

        //解析各个字段
        uint32_t crc_stored = read_be32(buf); //偏移 0
        uint8_t type = static_cast<uint8_t>(buf[4]); //偏移 4
        uint32_t klen = read_be32(buf+5); //偏移 5
        uint32_t vlen = read_be32(buf+9); //偏移 9

        //合法性检查
        if(type != static_cast<uint8_t>(RecordType::Set) && type != static_cast<uint8_t>(RecordType::Del))
        {
            return false;
        }

        //若超过上限，可能是因为损坏
        if(klen > MAX_KEY || vlen > MAX_VALUE)
        {
            return false;
        }

        //检查内容是否到齐
        size_t total = 13 + static_cast<size_t>(klen) + vlen;
        if(len < total)
        {
            return false;
        }

        //CRC 校验 type 起 (buf+4)，共 9 + klen + vlen 字节
        //9 = type(1) + key_len(4) + val_len(4)
        uint32_t crc_calc = crc32(buf+4,9+klen + vlen);
        if(crc_calc != crc_stored)
        {
            return false;
        }

        //全部校验都通过则填写输出参数
        out.type = static_cast<RecordType>(type);
        out.key.assign(buf+13,klen); //清空并重新赋值
        out.value.assign(buf+13+klen,vlen); //清空并重新赋值

        consumed = total; //告诉调用方该记录占用多少字节

        return true;

    }

    Storage::Storage(const std::string &path)
    {
        fd_ = open(path.c_str(),O_RDWR | O_CREAT,0644);

        if(fd_ < 0)
        {
            std::cerr << "open " << path << " failed: " << strerror(errno) << std::endl;
            return; //未打开成功 fd_ 仍等于 -1 good() == false
        }

        //lseek 获取文件大小
        off_t size = lseek(fd_,0,SEEK_END);

        if(size < 0)
        {
            std::cerr << "lseek failed" << std::endl;
            close(fd_);
            fd_ = -1;
            return;
        }

        //即新建的全新文件
        if(size == 0)
        {
            //显式构造一个名为 header 的字符串对象，精准复制 MAGIC
            std::string header(MAGIC,sizeof(MAGIC));
            append_be32(header,FORMAT_VERSION);

            //如果 write_all 成功了，调用 fsync 将这 8 字节强制刷到磁盘中
            if(!write_all(fd_,header.data(),header.size()) || fsync(fd_) != 0)
            {
                std::cerr << "write file header failde" << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }

        }
        else //已有文件 校验头
        {
            if(size < static_cast<off_t>(FILE_HEADER_SIZE))
            {
                std::cerr << "file too small to be a kv data file" << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }

            char hdr[FILE_HEADER_SIZE];
            if(!read_exact(fd_,hdr,sizeof(hdr),0))
            {
                std::cerr << "read file header failed" << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }


            if(memcmp(hdr,MAGIC,sizeof(MAGIC)) != 0)
            {
                std::cerr << "not a kv data file (bad magic)" << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }

            uint32_t ver = read_be32(hdr+4);
            if(ver != FORMAT_VERSION)
            {
                std::cerr << "unsupported format version : " << ver << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }



        }


    }



    Storage::~Storage()
    {
        if(fd_ >= 0)
        {
            close(fd_);
        }
    }


    bool Storage::put(const std::string &key,const std::string &value)
    {
        //合法性检查
        if(key.empty() || key.size() > MAX_KEY || value.size() > MAX_VALUE)
        {
            return false;
        }

        //编码 转换为 SET 记录
        std::string rec = encode_record(RecordType::Set,key,value);

        //记录落在文件末尾，先取当前位置作为偏移 (追加)
        off_t offset = lseek(fd_,0,SEEK_END);
        if(offset < 0)
        {
            return false;
        }

        //追加写
        if(!write_all(fd_,rec.data(),rec.size()))
        {
            return false;
        }

        //按照策略
        if(sync_on_put_ && fsync(fd_) != 0)
        {
            return false;
        }

        //更新内存索引 指向最新
        index_[key] = IndexEntry{static_cast<uint64_t>(offset),static_cast<uint32_t>(rec.size())};

        return true;

    }


    void Storage::dump_index(std::ostream &os) const
    {
        for(const auto &kv : index_)
        {
            os << " " << kv.first << " -> offset = " << kv.second.offset << " len = " << kv.second.total_len << "\n";
        }
    }















}//namespace kv
