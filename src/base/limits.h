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
}

#endif
