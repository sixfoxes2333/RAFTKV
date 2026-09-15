#ifndef BUFFER_H
#define BUFFER_H
#include<vector>
#include<unistd.h>
#include<cstring>


class Buffer
{
public:
    size_t readable() const;  //w-r 即还有多少可读
    size_t writable() const; //还有多少可写
    const char* peek() const; //可读区的起点
    void retrieve(size_t n); //消费 n 字节
    void append(const char *data,size_t len); //推进 w 追加到尾部

private:
    std::vector<char>buf_;
    size_t read_idx_ = 0; //标记已消费
    size_t write_idx_ = 0; //标记可写
};


#endif