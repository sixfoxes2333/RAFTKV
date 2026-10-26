#include "logstore.h"
#include "base/be32.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <limits>
#include <utility>
#include <sys/stat.h>
#include <unistd.h>

namespace raft
{

    namespace
    {
        constexpr std::size_t HEADER_SIZE = 32; //固定头部长度
        constexpr std::size_t CRC_SIZE = 4; //尾部命令 CRC 长度
        constexpr std::uint32_t FORMAT_VERSION = 1; 
        constexpr char MAGIC[] = "RLOG"; //文件格式标识

        //临时 fd 的 RAII 管理器 中途任意 return 则自动 close
        class ScopedFd
        {
        public:
            explicit ScopedFd(int fd) : fd_(fd)
            {

            }

            ~ScopedFd()
            {
                if(fd_ >= 0)
                {
                    ::close(fd_);
                }
            }

            //禁止复制
            ScopedFd(const ScopedFd&) = delete;
            ScopedFd& operator=(const ScopedFd&) = delete;

            //借出 fd 供系统调用，所有权仍旧在于 ScopedFd 中
            int get()const 
            {
                return fd_;
            }

            //交出 fd_ 控制权
            int release()
            {
                int result = fd_;
                fd_ = -1;
                return result;
            }


        private:
            int fd_;

        };

        void system_error(std::string& error,const char* operation)
        {
            const int saved = errno;
            error = std::string(operation) + ": " + std::strerror(saved);
        }

        //计算前 size 字节的标准 CRC32
        std::uint32_t crc32(const char* data,std::size_t size)
        {
            static const auto table = []
            {
                std::array<std::uint32_t,256>result{};

                for(std::int32_t i = 0 ; i < 256 ; i++)
                {
                    std::uint32_t value = i;

                    for(int bit = 0 ; bit < 8 ; bit++)
                    {
                        value = (value & 1) ? (value >> 1) ^ 0xEDB88320u : value >> 1;
                    }

                    result[i] = value;
                }

                return result;
            }();

            std::uint32_t value = 0xFFFFFFFFu;

            for(std::size_t i = 0 ; i < size ; i++)
            {
                value = table[(value ^ static_cast<unsigned char>(data[i])) & 0xFFu] ^ (value >> 8);
            }

            return value ^ 0xFFFFFFFFu;

        }

        struct Header
        {
            std::uint64_t index; //记录逻辑序号
            std::uint64_t term; //记录历史任期
            std::uint32_t command_size; //不含 CRC 的需读取的命令字节数
        };

        //验证并解码一个 Header
        bool decode_header(const char* bytes,Header& out,std::string& error)
        {
            if(crc32(bytes,28) != kv::read_be32(bytes + 28))
            {
                error = "raft.log : header CRC mismatch";
                return false;
            }

            if(std::memcmp(bytes,MAGIC,4) != 0)
            {
                error = "raft.log: bad magic";
                return false;
            }

            if(kv::read_be32(bytes + 4) != FORMAT_VERSION)
            {
                error = "raft.log : unsupported format version";
                return false;
            }

            Header header
            {
                kv::read_be64(bytes + 8),
                kv::read_be64(bytes + 16),
                kv::read_be32(bytes + 24)
            };

            //虚拟初始边界 必须均大于 0
            if(header.index == 0 || header.term == 0)
            {
                error = "raft.log entry index/term must be nonzero";
                return false;
            }

            if(header.command_size > LogStore::MAX_COMMAND_SIZE)
            {
                error = "raft.log : command length exceeds limit";
                return false;
            }

            out = header;
            return true;

        }

        //编码
        void encode_header(const LogEntry& entry,std::string& bytes)
        {
            bytes.resize(HEADER_SIZE + entry.command.size() + CRC_SIZE);

            std::memcpy(bytes.data(),MAGIC,4);
            kv::put_be32(bytes.data() + 4 , FORMAT_VERSION);
            kv::put_be64(bytes.data() + 8,entry.index);
            kv::put_be64(bytes.data() + 16,entry.term);
            kv::put_be32(bytes.data() + 24,static_cast<std::uint32_t>(entry.command.size()));
            kv::put_be32(bytes.data() + 28,crc32(bytes.data(),28));

            std::memcpy(bytes.data() + HEADER_SIZE,entry.command.data(),entry.command.size());

            kv::put_be32(bytes.data() + HEADER_SIZE + entry.command.size(),crc32(entry.command.data(),entry.command.size()));

        }

        //完整读
        bool read_full(int fd,char* out,std::size_t size,off_t offset,std::string& error)
        {
            std::size_t done = 0;

            while(done < size)
            {
                const ssize_t n = ::pread(fd,out+done,size-done,offset+static_cast<off_t>(done));

                if(n < 0)
                {
                    if(errno == EINTR)
                    {
                        continue;
                    }

                    system_error(error,"pread raft.log");
                    return false;
                }

                if(n == 0)
                {
                    error = "pread raft.log: unexpected EOF";
                    return false;
                }

                done += static_cast<std::size_t>(n);
            }

            return true;

        }

        //完整写
        bool write_full(int fd,const char* bytes,std::size_t size,off_t offset,std::string& error)
        {
            std::size_t done = 0;
            
            while(done < size)
            {
                const ssize_t n = ::pwrite(fd,bytes + done,size-done,offset + static_cast<off_t>(done));
                

                if(n < 0)
                {
                    if(errno == EINTR)
                    {
                        continue;
                    }

                    system_error(error,"pwrite raft.log");
                    return false;

                }

                if(n == 0)
                {
                    error = "pwrite raft.log: zero progress";
                    return false;
                }

                done += static_cast<std::size_t>(n);

            }

            return true;

        }

        
        bool sync_fd(int fd,const char* operation,std::string& error)
        {
            while(::fsync(fd) != 0)
            {
                if(errno == EINTR)
                {
                    continue;
                }

                system_error(error,operation);

                return false;
            }

            return true;
        }


        //将文件长度改为 length ，删除后缀或者修复残尾
        bool truncate_fd(int fd,off_t length,std::string& error)
        {
            while(::ftruncate(fd,length) != 0)
            {
                if(errno == EINTR)
                {
                    continue;
                }

                system_error(error,"ftruncate raft.log");
                return false;
            }

            return true;
        }

        //获取文件长度
        bool file_size(int fd,off_t& out,std::string& error)
        {
            struct stat st{};

            if(::fstat(fd,&st) != 0)
            {
                system_error(error,"fstat raft.log");
                return false;
            }

            if(!S_ISREG(st.st_mode))
            {
                error = "raft.log must be a regular file";
                return false;
            }

            out = st.st_size;

            return true;
        }


        //offset 指向命令起点 size 只算 命令 不含尾 CRC
        bool read_command(int fd,off_t offset,std::uint32_t size,std::string& out,std::string& error)
        {
            std::string bytes;

            try
            {
                bytes.resize(static_cast<std::size_t>(size) + CRC_SIZE);
            }
            catch(const std::exception& ex)
            {
                error = std::string("allocate command buffer: ") + ex.what();
                return false;
            }

            if(!read_full(fd,bytes.data(),bytes.size(),offset,error))
            {
                return false;
            }

            if(crc32(bytes.data(),size) != kv::read_be32(bytes.data() + size))
            {
                error = "raft.log: command CRC mismatch";
                return false;
            }

            //去掉尾 CRC
            bytes.resize(size);
            out = std::move(bytes);
            return true;

        }


    };//匿名命名空间

    LogStore::LogStore(const std::string& directory):directory_(directory)
    {

    }

    LogStore::~LogStore()
    {
        if(fd_ >= 0)
        {
            ::close(fd_);
        }
    }

    bool LogStore::good()const
    {
        return fd_ >= 0 && !poisoned_;
    }

    //当前最后一个日志序号
    std::uint64_t LogStore::last_index()const
    {
        return static_cast<std::uint64_t>(meta_.size());
    }

    bool LogStore::require_ready(std::string& error)const
    {
        if(poisoned_)
        {
            error = "LogStore is poisoned; stop using it and restart";
            return false;
        }

        if(fd_ < 0)
        {
            error = "Logstore is not open";
            return false;
        }

        return true;
    }

    bool LogStore::poison()
    {
        poisoned_ = true;
        return false;
    }

    bool LogStore::create(std::string& error)
    {
        return open_file(true,error);
    }

    bool LogStore::open(std::string& error)
    {
        return open_file(false,error);
    }

    //open 与 create 的共同实现
    bool LogStore::open_file(bool create_new,std::string& error)
    {
        error.clear();

        if(fd_ >= 0 || poisoned_)
        {
            error = "LogStore is already open or poisoned";
            return false;
        }

        ScopedFd directory(::open(directory_.c_str(),O_RDONLY | O_DIRECTORY | O_CLOEXEC));

        if(directory.get() < 0)
        {
            system_error(error,"open log directory");
            return false;
        }

        int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW;

        if(create_new)
        {
            flags |= O_CREAT | O_EXCL;
        }

        ScopedFd file(::openat(directory.get(),"raft.log",flags,0600));

        if(file.get() < 0)
        {
            system_error(error,"open raft.log");
            return false;
        }

        off_t size = 0;
        if(!file_size(file.get(),size,error))
        {
            return poison();
        }

        //新建文件
        if(create_new)
        {   //同步文件 然后再同步父目录
            if(!sync_fd(file.get(),"fsync new raft.log",error) || !sync_fd(directory.get(),"fsync log directory",error))
            {
                return poison();
            }

            fd_ = file.release();
            return true;

        }

        fd_ = file.release();

        if(!recover(error) || !sync_fd(directory.get(),"fsync log directory",error))
        {
            ::close(fd_);
            fd_ = -1;
            return poison();
        }

        return true;

    }

    //从头扫描已有日志：验证完整记录，重建定位索引。修复残尾
    bool LogStore::recover(std::string& error)
    {
        off_t size = 0;
        if(!file_size(fd_,size,error))
        {
            return false;
        }

        //用于临时索引 全部成功再转移到 Meta
        std::vector<Meta>recovered;

        off_t offset = 0; //下一条待扫描记录的起始位置

        std::uint64_t previous_term = 0;

        while(offset < size)
        {
            const auto remaining = static_cast<std::uint64_t>(size-offset);

            if(remaining < HEADER_SIZE)
            {
                break;
            }

            char bytes[HEADER_SIZE];
            Header header{};

            //CRC/字段非法
            if(!read_full(fd_,bytes,HEADER_SIZE,offset,error) || !decode_header(bytes,header,error))
            {
                return false;
            }


            const auto expected = static_cast<std::uint64_t>(recovered.size())+1;

            if(header.index != expected || header.term < previous_term)
            {
                error = "raft.log: noncontigous index or decreasing term";
                return false;
            }

            const std::size_t total = HEADER_SIZE + static_cast<std::size_t>(header.command_size) + CRC_SIZE;

            //残尾
            if(total > remaining)
            {
                break;
            }

            std::string command;

            if(!read_command(fd_,offset + static_cast<off_t>(HEADER_SIZE),header.command_size,command,error))
            {
                return false;
            }

            try
            {
                recovered.push_back({header.term,offset,header.command_size});
            }
            catch(const std::exception& ex)
            {
                error = std::string("allocate log index: ") + ex.what();
                return false;
            }

            previous_term = header.term;

            offset += static_cast<off_t>(total);

        }

        //去除残尾
        if(offset != size)
        {
            if(!truncate_fd(fd_,offset,error))
            {
                return false;
            }
        }

        if(!sync_fd(fd_,"fsync recovered raft.log",error))
        {
            return false;
        }

        meta_ = std::move(recovered);
        end_offset_ = offset;
        return true;

    }


    bool LogStore::append(const LogEntry& entry,std::string& error)
    {
        std::vector<LogEntry>entries;

        try
        {
            entries.push_back(entry);
        }
        catch(const std::exception& ex)
        {
            error = std::string("allocate append batch: ") + ex.what();
            return false;
        }

        return append_batch(entries,error);
    }

    bool LogStore::append_batch(const std::vector<LogEntry>& entries,std::string& error)
    {
        error.clear();

        if(!require_ready(error))
        {
            return false;
        }

        if(entries.empty())
        {
            return true;
        }

        std::vector<Meta> pending;

        try
        {
            pending.reserve(entries.size());
        }
        catch(const std::exception& ex)
        {
            error = std::string("allocate log index: ") + ex.what();
            return false;
        }

        std::uint64_t expected = last_index()+1;
        std::uint64_t previous_term = meta_.empty() ? 0 : meta_.back().term;
        off_t offset = end_offset_;
        std::size_t total = 0;

        for(const LogEntry& entry:entries)
        {
            if(entry.index != expected)
            {
                error = "append: log index must be continuous";
                return false;
            }

            if(entry.term == 0 || entry.term < previous_term)
            {
                error = "append log term must be nonzero and nondecreasing";
                return false;
            }

            if(entry.command.size() > MAX_COMMAND_SIZE)
            {
                error = "append: command length exceeds limit";
                return false;
            }

            const std::size_t record_size = HEADER_SIZE + entry.command.size() + CRC_SIZE;

            if(offset > std::numeric_limits<off_t>::max() - static_cast<off_t>(record_size))
            {
                error = "append: raft.log offset would overflow";
                return false;
            }

            pending.push_back({entry.term,offset,static_cast<std::uint32_t>(entry.command.size())});

            offset += static_cast<off_t>(record_size);
            total += record_size;
            previous_term = entry.term;
            ++expected;

        }

        const std::size_t needed = meta_.size() + entries.size();

        try
        {
            if(needed > meta_.capacity())
            {
                std::size_t next = meta_.capacity() == 0 ? needed : meta_.capacity();

                while(next < needed)
                {
                    next *= 2;
                }

                meta_.reserve(next);
            }
        }
        catch(const std::exception& ex)
        {
            error = std::string("allocate log index: ") + ex.what();
            return false;
        }

        //复用一个记录缓冲: 容量按整批预留，逐条编码时不会重新分配
        std::string buffer;

        try
        {
            buffer.reserve(total);
        }
        catch(const std::exception& ex)
        {
            error = std::string("allocate log record: ") + ex.what();
            return false;
        }

        offset = end_offset_;

        for(const LogEntry& entry : entries)
        {
            encode_header(entry,buffer);

            if(!write_full(fd_,buffer.data(),buffer.size(),offset,error))
            {
                return poison();
            }

            offset += static_cast<off_t>(buffer.size());
        }

        if(!sync_fd(fd_,"fsync appended raft.log",error))
        {
            return poison();
        }
        
        meta_.insert(meta_.end(),pending.begin(),pending.end());
        end_offset_ = offset;

        return true;

    }

    //从磁盘读取一条完整日志并校验
    bool LogStore::get(std::uint64_t index,LogEntry& out,std::string& error)
    {
        error.clear();

        if(!require_ready(error))
        {
            return false;
        }

        if(index == 0 || index > last_index())
        {
            error = "get: log index out of range";
            return false;
        }

        //meta_ 只保存定位信息 命令内容永远从磁盘读取
        const Meta& meta = meta_[static_cast<std::size_t>(index-1)];

        char bytes[HEADER_SIZE];
        Header header{};

        if(!read_full(fd_,bytes,HEADER_SIZE,meta.offset,error) || !decode_header(bytes,header,error))
        {
            return poison();
        }

        //磁盘记录必须与内存索引一致 否则索引不可信
        if(header.index != index || header.term != meta.term || header.command_size != meta.command_size)
        {
            error = "get: record does not match log index";
            return poison();
        }

        //先写局部变量 失败时不污染调用方的 out
        LogEntry entry;
        entry.index = header.index;
        entry.term = header.term;

        if(!read_command(fd_,meta.offset + static_cast<off_t>(HEADER_SIZE),header.command_size,entry.command,error))
        {
            return poison();
        }

        out = std::move(entry);
        return true;

    }

    //从内存索引查询某条日志的 term
    bool LogStore::term_at(std::uint64_t index,std::uint64_t& out,std::string& error)const
    {
        error.clear();

        if(!require_ready(error))
        {
            return false;
        }

        if(index == 0 || index > last_index())
        {
            error = "term_at: log index out of range";
            return false;
        }

        out = meta_[static_cast<std::size_t>(index-1)].term;
        return true;

    }

    //删除 from_index 及其之后的全部日志
    bool LogStore::truncate_suffix(std::uint64_t from_index,std::string& error)
    {
        error.clear();

        if(!require_ready(error))
        {
            return false;
        }

        if(from_index == 0 || from_index > last_index() + 1)
        {
            error = "truncate_suffix: log index out of range";
            return false;
        }

        //无可删记录
        if(from_index == last_index() + 1)
        {
            return true;
        }

        const std::size_t keep = static_cast<std::size_t>(from_index - 1);
        const off_t offset = meta_[keep].offset;

        if(!truncate_fd(fd_,offset,error))
        {
            return poison();
        }

        //必须先确认删除已经持久化，之后才允许覆盖这之后的位置
        if(!sync_fd(fd_,"fsync truncated raft.log",error))
        {
            return poison();
        }

        meta_.resize(keep);
        end_offset_ = offset;

        return true;

    }





};//namespace raft