#include"../src/storage.h"
#include<fstream>
#include<iostream>
#include<iterator>
#include<string>

//简易检查宏 程序持续跑完 打印失败在第几行  统一返回失败码
static int g_failed = 0;
#define CHECK(cond)                                                             \
    do {                                                                        \
        if (cond)                                                               \
            std::cout << "  ok   : " #cond << std::endl;                                  \
        else {                                                                  \
            std::cout << "  FAIL : " #cond "   (line " << __LINE__ << ")"            \
                 << std::endl;                                                       \
            g_failed++;                                                         \
        }                                                                       \
    } while (0)



//十六进制显示
//将二进制字节流转换为 十六进制文本形式
static std::string hex(const std::string &s)
{
    static const char* digits = "0123456789abcdef";
    std::string out;
    for(unsigned char c : s )
    {
        out += digits[c >> 4];
        out += digits[c & 0x0F];
        out += ' ';
    }

    if(!out.empty())
    {
        out.pop_back();
    }
    return out;
}


int main()
{
    //测试 CRC32 标准校验值
    std::cout << "[1] CRC32 标准值" << std::endl;
    CHECK(kv::crc32("123456789",9) == 0xCBF43926u);

    //SET 记录编码
    std::cout << "[2] SET 记录编码" << std::endl;
    std::string r1 = kv::encode_record(1,"a","1");
    CHECK(r1.size() == 15);
    CHECK(hex(r1) == "20 84 55 73 01 00 00 00 01 00 00 00 01 61 31");

    //DEL 记录编码
    std::cout << "[3] DEL 记录编码" << std::endl;
    std::string r2 = kv::encode_record(2,"a","");
    CHECK(r2.size() == 14);
    CHECK(hex(r2) == "16 96 3a 20 02 00 00 00 01 00 00 00 00 61");

    //写文件
    std::cout << "[4] 写文件 / 读回 / 解码" << std::endl;
    const char* path = "/tmp/test_storage.log";
    {
        std::ofstream f(path,std::ios::binary);
        f.write(r1.data(),r1.size());
        f.write(r2.data(),r2.size());
    }

    std::ifstream f(path,std::ios::binary);
    std::string all((std::istreambuf_iterator<char>(f)),std::istreambuf_iterator<char>());
    CHECK(all.size() == r1.size() + r2.size());

    kv::Record rec;
    size_t consumed = 0;

    // 第一条
    CHECK(kv::decode_record(all.data(), all.size(), rec, consumed));
    CHECK(rec.type == 1);
    CHECK(rec.key == "a");
    CHECK(rec.value == "1");
    CHECK(consumed == 15);

    // 第二条：从 consumed 处继续
    CHECK(kv::decode_record(all.data() + consumed, all.size() - consumed, rec, consumed));
    CHECK(rec.type == 2);
    CHECK(rec.key == "a");
    CHECK(rec.value == "");
    CHECK(consumed == 14);

    //坏输入必须被拒绝
    std::cout << "[5] 坏输入拒绝" << std::endl;

    // 5a 只有 10 字节：连 13 字节的最小头都不够 → 第一道检查拦下
    CHECK(kv::decode_record(r1.data(), 10, rec, consumed) == false);

    // 5b 正好 13 字节：头够了，但 key+value 的字节还没到 → 第三道检查拦下
    CHECK(kv::decode_record(r1.data(), 13, rec, consumed) == false);

    // 5c 篡改 key_len 的第一个字节（偏移 5）：
    //    长度变成 0xFF000001，超过 MAX_KEY → 上限检查拦下
    std::string bad_len = r1;
    bad_len[5] = static_cast<char>(0xFF);
    CHECK(kv::decode_record(bad_len.data(), bad_len.size(), rec, consumed) == false);

    // 5d 篡改 value 的一个字节（偏移 14）：
    //    长度字段都合法，只有内容变了 → 必须由 CRC 拦下
    std::string bad_val = r1;
    bad_val[14] ^= 0x01;
    CHECK(kv::decode_record(bad_val.data(), bad_val.size(), rec, consumed) == false);

    //汇总
    if (g_failed == 0) 
    {
        std::cout << "\nall tests passed" << std::endl;
        return 0;
    }
    std::cout << "\n" << g_failed << " check(s) FAILED" << std::endl;
    return 1;

}