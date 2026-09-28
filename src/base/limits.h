#ifndef BASE_LIMITS_H
#define BASE_LIMITS_H

#include<cstddef>

//全局上限：网络帧 / 协议字段 / 存储记录共用同一套常量
//（以前 MAX_KEY / MAX_VALUE 写在 storage/record.h 里、MAX_MESSAGE 写在 main.cpp 里，
//  三处各写一份容易漂移；收敛到这里，谁需要谁 include）
namespace kv
{
    inline constexpr size_t MAX_MESSAGE = 16 * 1024 * 1024; //一个网络帧的最大长度 16MB（防恶意长度炸内存）
    inline constexpr size_t MAX_KEY     = 64 * 1024;        //key 最大 64KB
    inline constexpr size_t MAX_VALUE   = 16 * 1024 * 1024; //value 最大 16MB（与 MAX_MESSAGE 对应）

    // 后台队列字节预算 限流
    inline constexpr size_t MAX_QUEUE_BYTES = 64*1024*1024; // 64MB
    
    // 一条连接不能独占全局的预算
    inline constexpr size_t MAX_CONN_QUEUE_BYTES = 32*1024*1024; //32MB

    inline constexpr size_t MAX_IN_BUFFER_BYTES = 128*1024*1024; //128MB

}

#endif
