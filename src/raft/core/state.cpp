#include "raft/core/state.h"
#include "storage/record.h" // 复用 crc 32
#include "base/be32.h"
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <cstdio>
#include <sys/stat.h>
#include <unistd.h>

namespace raft
{
    //匿名命名空间 仅在 state.cpp 内使用
    namespace
    {
        constexpr std::size_t STATE_SIZE = 28;
        constexpr std::size_t CRC_OFFSET = 24;
        constexpr std::uint32_t FORMAT_VERSION = 1;
        using StateBytes = std::array<char,STATE_SIZE>;

        class ScopedFd
        {
        public:
            explicit ScopedFd(int fd):fd_(fd)
            {

            }

            ~ScopedFd()
            {
                if(fd_ >= 0)
                {
                    ::close(fd_);
                }
            }

            //不允许复制与移动
            ScopedFd(const ScopedFd&) = delete;
            ScopedFd& operator=(const ScopedFd&) = delete;
            ScopedFd(ScopedFd&&) = delete;
            ScopedFd& operator=(ScopedFd&&) = delete;


            int get()const
            {
                return fd_;
            }

            //交出 fd 的所有权 交出后，析构不会关闭
            int release()
            {
                int fd = fd_;
                fd_ = -1;
                return fd;
            }

        private:
            int fd_;
            
        };

        //保存 error
        bool system_error(std::string& error,const char* operation,const std::string& path)
        {
            const int code = errno;

            error = std::string(operation) + " " + path + ": " + std::strerror(code);

            return false;
        }

        //大端整数编解码


        //编码为固定长度的磁盘记录
        StateBytes encode(const HardState& state)
        {
            StateBytes bytes{};

            std::memcpy(bytes.data(),"RFST",4);
            kv::put_be32(bytes.data()+4,FORMAT_VERSION);
            kv::put_be64(bytes.data()+8,state.current_term);
            kv::put_be64(bytes.data()+16,state.voted_for);

            const auto crc = kv::crc32(reinterpret_cast<const char*>(bytes.data()),CRC_OFFSET);

            kv::put_be32(bytes.data() + CRC_OFFSET,crc);
            return bytes;
        }


        //完整读写与同步

        //完整读
        bool read_full(int fd,StateBytes& bytes,const std::string& path,std::string& error)
        {
            std::size_t done = 0;

            while(done < bytes.size())
            {
                const ssize_t n = ::read(fd,bytes.data()+done,bytes.size()-done);

                if(n > 0)
                {
                    done += static_cast<std::size_t>(n);
                }
                else if(n < 0 && errno == EINTR)
                {
                    continue;
                }
                else if(n == 0)
                {
                    error = "unexpected EOF: " + path;
                    return false;
                }
                else
                {
                    return system_error(error,"read",path);
                }
            }

            return true;
        }

        //完整写
        bool write_full(int fd,const StateBytes& bytes,const std::string& path,std::string& error)
        {
            std::size_t done = 0;

            while(done < bytes.size())
            {
                const ssize_t n = ::write(fd,bytes.data()+done,bytes.size()-done);

                if(n > 0)
                {
                    done += static_cast<std::size_t>(n);
                }
                else if(n < 0 && errno == EINTR)
                {
                    continue;
                }
                else if(n == 0)
                {
                    error = "write made no progress: " + path;
                    return false;
                }
                else
                {
                    return system_error(error,"write",path);
                }
            }

            return true;
        }

        //fsync 被 EINTR 打断则重试
        bool sync_fd(int fd,const std::string& path,std::string& error)
        {
            for( ; ; )
            {
                if(::fsync(fd) == 0)
                {
                    return true;
                }

                if(errno != EINTR)
                {
                    return system_error(error,"fsync",path);
                }
            }
        }


    };//匿名命名空间

    StateStore::StateStore(const std::string& directory) : directory_(directory)
    {

    }

    bool StateStore::load(HardState& out,std::string& error)const
    {
        error.clear();

        if(directory_.empty())
        {
            error = "state directory is empty";
            return false;
        }

        const std::string path = directory_ + "/raft.state";

        //O_NOFOLLOW：拒绝正式文件路径是符号链接的情况
        //O_CLOEXEC：exec 后不泄露这个 fd
        ScopedFd file(::open(path.c_str(),O_RDONLY | O_CLOEXEC | O_NOFOLLOW)); 

        if(file.get() < 0)
        {
            return system_error(error,"open",path);
        }

        //检查文件类型与长度
        //拒绝目录，半条记录，带多余字节的记录
        struct stat st{};

        if(::fstat(file.get(),&st) != 0)
        {
            return system_error(error,"fstat",path);
        }

        //是否为普通文件 大小是否严格相等
        if(!S_ISREG(st.st_mode) || st.st_size != static_cast<off_t>(STATE_SIZE))
        {
            error = "invalid state file type or length: " + path;
            return false;
        }

        StateBytes bytes{};

        if(!read_full(file.get(),bytes,path,error))
        {
            return false;
        }

        if(std::memcmp(bytes.data(),"RFST",4) != 0)
        {
            error = "invalid state magic: " + path;
            return false;
        }

        if(kv::read_be32(bytes.data() + 4) != FORMAT_VERSION)
        {
            error = "unsupported state version: " + path;
            return false;
        }

        const auto expected_crc = kv::read_be32(bytes.data() + CRC_OFFSET);

        const auto actual_crc = kv::crc32(reinterpret_cast<const char*>(bytes.data()),CRC_OFFSET);

        if(actual_crc != expected_crc)
        {
            error = "state CRC mismatch: " + path;
            return false;
        }

        //所有检查通过 更新输出
        const HardState decoded
        {
           kv::read_be64(bytes.data()+8),kv::read_be64(bytes.data()+16)
        };

        out = decoded;

        return true;

    }

    bool StateStore::save(const HardState& state,std::string& error)const
    {
        error.clear();

        if(directory_.empty())
        {
            error = "state directory is empty";
            return false;
        }

        const std::string path = directory_ + "/raft.state";
        const std::string temp = directory_ + "/raft.state.tmp";

        ScopedFd directory(::open(directory_.c_str(),O_RDONLY | O_DIRECTORY | O_CLOEXEC));

        if(directory.get() < 0)
        {
            return system_error(error,"open directory",directory_);
        }

        ScopedFd file(::open(temp.c_str(),O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW,0600));

        if(file.get() < 0)
        {
            return system_error(error,"open",temp);
        }

        const StateBytes bytes = encode(state);

        //完整写入，再同步 temp 内容
        if(!write_full(file.get(),bytes,temp,error) || !sync_fd(file.get(),temp,error))
        {
            ::unlink(temp.c_str());
            return false;
        }

        //替换前，检查 close 结果
        const int temp_fd = file.release();

        if(::close(temp_fd) != 0)
        {
            system_error(error,"close",temp);
            ::unlink(temp.c_str());
            return false;
        }

        //替换正式文件名
        if(::rename(temp.c_str(),path.c_str()) != 0)
        {
            system_error(error,"rename",path);
            ::unlink(temp.c_str());
            return false;
        }

        return sync_fd(directory.get(),directory_,error);

    }



};//namespace raft