#include "base/buffer.h"

size_t Buffer::readable()const
{
    return write_idx_-read_idx_;
}

size_t Buffer::writable()const
{
    return buf_.size()-write_idx_;
}


const char* Buffer::peek()const
{
    return buf_.data() + read_idx_; //.data 返回一个指向该向量内部连续内存存储区首地址的直接指针
}

void Buffer::retrieve(size_t n)
{
    if(n >= readable()) //消费掉全部或者更多
    {
        read_idx_ = 0;
        write_idx_ = 0;
    }
    else
    {
        read_idx_ += n;
    }
}

void Buffer::append(const char *data,size_t len)
{
    if(writable() < len) //空闲空间不够
    {
        //尝试把已消费数据挪走
        if(read_idx_ + writable() >= len)
        {
            size_t readable_len = readable();
            memmove(buf_.data(),buf_.data() + read_idx_,readable_len);
            read_idx_ = 0;
            write_idx_ = readable_len;
        }
        else //如果挪走也不够用
        {
            buf_.resize(write_idx_ + len);
        }
    }

    memcpy(buf_.data() + write_idx_,data,len);
    write_idx_ += len;
}