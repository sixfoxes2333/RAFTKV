#ifndef RAFT_LOGSTORE_H
#define RAFT_LOGSTORE_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <sys/types.h>


namespace raft
{

    struct LogEntry
    {
        std::uint64_t index = 0;//命令在日志序列中的位置
        std::uint64_t term = 0; //创建这条日志时的任期
        std::string command;//命令的编码字符串
    };

    //raft.log index 从 1 开始编号
    //暂无快照 单线程使用 同一目录仅一位写者
    class LogStore
    {
    public:
        static constexpr std::size_t MAX_COMMAND_SIZE = 16*1024*1024; //一条 command 的最大字节数

        //仅保存路径
        explicit LogStore(const std::string& directory);
        ~LogStore();

        //不可复制，移动
        LogStore(const LogStore&) = delete;
        LogStore& operator=(const LogStore&) = delete;
        LogStore(LogStore&&) = delete;
        LogStore& operator=(LogStore&&) = delete;

        //创建空的 raft.log 若存在则失败
        bool create(std::string& error);

        //打开已有文件并恢复索引 缺失则报错 
        //截断残尾
        //恢复成功前重新 fsync 文件和父目录
        bool open(std::string& error);

        //查询是否已打开且未 poisoned
        bool good()const;

        //空日志返回 0 返回最后的日志序号
        std::uint64_t last_index()const;

        //单条 append 看作是 append_batch 的特殊情况
        bool append(const LogEntry& entry,std::string& error);
        bool append_batch(const std::vector<LogEntry>& entries,std::string& error);

        //从磁盘读取完整日志并校验
        bool get(std::uint64_t index,LogEntry& out,std::string& error);

        //从内存索引查询历史 term
        bool term_at(std::uint64_t index,std::uint64_t& out,std::string& error)const;

        // fsync 截断成功后更新索引 删除该 index 及后面的日志
        bool truncate_suffix(std::uint64_t from_index,std::string& error);


    private:
        //一条日志的定位信息
        struct Meta
        {
            std::uint64_t term; //对应日志的任期
            off_t offset;//记录在文件中的起始字节位置
            std::uint32_t command_size; //命令长度
        };

        bool open_file(bool create_new,std::string& error);
        bool recover(std::string& error);
        bool require_ready(std::string& error)const;  //检查操作
        bool poison();

        std::string directory_;
        int fd_ = -1;
        off_t end_offset_ = 0; //下一次写入的字节位置
        std::vector<Meta>meta_; // meta_.size() 当前日志条数，与最后的 index  保存所有日志的内存定位索引
        bool poisoned_ = false;

    };

};//namespace raft


#endif