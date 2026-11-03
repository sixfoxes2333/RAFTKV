#ifndef STORAGE_H
#define STORAGE_H
#include<string>
#include<cstdint>
#include<unordered_map>
#include<cstddef>
#include<ostream>
#include "storage/record.h" 
#include<sys/types.h>


//命名空间防止和其他库同名冲突
namespace kv
{

    //索引条目 bitcask keydir
    struct IndexEntry
    {
        uint64_t offset; //记录起始偏移量
        uint32_t total_len; //记录总字节数
    };


    class Storage
    {
    public:

    enum class OpenMode
    {
        Recover, //打开并恢复已有数据库
        Rebuild //清空重建
    };


        explicit Storage(const std::string &path,OpenMode mode = OpenMode::Recover); //禁用隐式转换，打开/新建文件，顺便写上 recover() 以重建 index_
        ~Storage();


        Storage(const Storage&) = delete;
        Storage& operator=(const Storage&) = delete;
        Storage(Storage&&) = delete;
        Storage& operator=(Storage&&) = delete;

        //查询内存索引
        //worker 区分 key 不存在 与 操作失败
        //get/del 返回 false 可能是 NotFound 也可可能是 CRC/IO 错误
        bool contains(const std::string &key)const
        {
            return index_.find(key) != index_.end();
        }


        bool good() const
        {
            return fd_ >= 0 && !poisoned_; //打开是否成功
        }

        bool put(const std::string &key,const std::string &value); //encode 并追加写入或更新已存在的 key

        //测试用函数 索引里有多少 key
        size_t key_count() const
        {
            return index_.size();
        }

        //测试用函数 打印索引内容
        void dump_index(std::ostream &os) const;

        bool get(const std::string &key,std::string &value); //根据 key 查询 value
        bool del(const std::string &key); //删除指定 key 并追加墓碑标记
 
        //延迟落盘 追加写 
        bool put_deferred(const std::string &key,const std::string &value);
        bool del_deferred(const std::string &key);

        //将已追加写的未落盘数据 fsync
        bool flush();


    private:
        int fd_ = -1;
        std::unordered_map<std::string,IndexEntry> index_;
        bool recover(); //数据库启动时显式调用的初始化函数，从磁盘重建内存索引
        off_t append_record(const std::string &bytes); //追加写入 返回起始偏移 失败返回 -1

        bool sync_on_put_ = true; //每条 put 是否立刻写入到磁盘

        bool poisoned_ = false; //写失败且结果无法确认 不可再用

    };

}//namespace kv


#endif