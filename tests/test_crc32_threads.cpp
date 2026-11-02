// CRC 的并发首次调用：本进程在放行线程前不调用生产 crc32。
#include "storage/record.h"
#include <atomic>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

int main()
{
    constexpr int count = 8;
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<int> wrong{0};
    std::vector<std::thread> threads;
    for(int i = 0; i < count; ++i)
    {
        threads.emplace_back([&]
        {
            ready.fetch_add(1);
            while(!go.load()) std::this_thread::yield();
            for(int n = 0; n < 64; ++n)
            {
                // 标准 CRC-32 黄金值，期望值不由被测函数生成。
                if(kv::crc32("123456789",9) != UINT32_C(0xcbf43926))
                {
                    wrong.fetch_add(1);
                }
            }
        });
    }
    while(ready.load() != count) std::this_thread::yield();
    go.store(true);
    for(auto& thread : threads) thread.join();
    if(wrong.load() != 0)
    {
        std::cerr << "FAIL CRC concurrent initialization mismatches=" << wrong.load() << '\n';
        return 1;
    }
    if(kv::crc32("",0) != 0)
    {
        std::cerr << "FAIL empty CRC\n";
        return 1;
    }
    std::cout << "crc32 concurrent first-call PASS (8 threads; golden CRC checked)\n";
    return 0;
}
