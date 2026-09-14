#ifndef STORAGE_H
#define STORAGE_H
#include<string>
#include<cstdint>
#include<unordered_map>
#include<cstddef>
#include<ostream>

//命名空间防止和其他库同名冲突
namespace kv
{
    const uint32_t MAX_KEY = 64*1024; //64KB 键最大大小
    const uint32_t MAX_VALUE = 16*1024*1024; //16MB 值最大大小 与网络层的 MAX_MESSAGE 16 MB 对应

    //类型记录码
    enum class RecordType : uint8_t
    {
        Set = 1, //写入/覆盖
        Del = 2, //删除墓碑
    };

    //数据包 作为 decode 的输出容器
    struct Record
    {
        RecordType type; //记录类型 1 = SET (写入/覆盖) 2 = DEL (删除墓碑)
        std::string key; //键
        std::string value; //值
    };

    //CRC32 计算检验
    uint32_t crc32(const char *data,size_t len);

    //编码 将记录转换为字节流 完整的一条记录包含 4 字节 CRC头 校验由 put() 负责
    std::string encode_record(RecordType type,const std::string &key,const std::string &value);

    //解码 将字节流转换为记录 buf 为起始位置 len 即 buf 里可用字节数 out 即解析成功时写入 comsumed 即写入成功时总共占用了多少字节
    bool decode_record(const char *buf,size_t len,Record &out,size_t &consumed);



    //索引条目
    struct IndexEntry
    {
        uint64_t offset; //记录起始偏移量
        uint32_t total_len; //记录总字节数
    };


    class Storage
    {
    public:

        explicit Storage(const std::string &path); //禁用隐式转换，打开/新建文件，顺便写上 recover() 以重建 index_
        ~Storage();

        bool good() const
        {
            return fd_ >= 0; //打开是否成功
        }

        bool put(const std::string &key,const std::string &value); //encode 并追加写入或更新已存在的 key

        //测试用函数 索引里有多少 key
        size_t key_count() const
        {
            return index_.size();
        }

        //测试用函数 打印索引内容
        void dump_index(std::ostream &os) const;

        bool get(const std::string &key,std::string &value); //根据 key 查询 valule
        bool del(const std::string &key); //删除指定 key 并追加墓碑标记


    private:
        int fd_ = -1;
        std::unordered_map<std::string,IndexEntry> index_;
        void recover(); //数据库启动时显式调用的初始化函数，从磁盘重建内存索引
        bool append_record(const std::string &bytes); //追加写入

        bool sync_on_put_ = true; //每条 put 是否立刻写入到磁盘

    };

}


#endif