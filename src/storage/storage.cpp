#include "storage/storage.h"
#include "base/be32.h" //文件头读写用到 append_be32 / read_be32
#include<cstring>
#include<fcntl.h>
#include<sys/file.h>
#include<unistd.h>
#include<cerrno>
#include<iostream>
#include<vector>
#include <sys/stat.h>

static const char MAGIC[4] = {'K','V','D','B'}; //magic
static const uint32_t FORMAT_VERSION = 2; //版本号
static const size_t FILE_HEADER_SIZE = sizeof(MAGIC) + sizeof(FORMAT_VERSION); //文件头 magic + 版本号 = 8 字节



//将 buf 的 len 字节全部写出 PoSIX 原生 write 存在短写与信号打断问题
static bool write_all(int fd,const char* buf,size_t len)
{
    size_t done = 0;
    while(done < len)
    {
        ssize_t n = write(fd,buf+done,len-done);
        if(n < 0)
        {
            //如果被信号打断
            if(errno == EINTR)
            {
                continue;
            }

            //否则为真错误
            return false;
        }

        if(n == 0)
        {
            return false;
        }

        done += static_cast<size_t>(n);
    }

    return true;
}


//从 offset 处读满 len 字节 保证从磁盘读取时不丢失，不发生多线程冲突
static bool read_exact(int fd,char* buf,size_t len,off_t offset)
{
    size_t done = 0;
    while(done < len)
    {
        //如果用 read ，多个线程同时对同一个 WAL 文件读取，互相篡改文件指针位置，会导致乱套
        //除非使用互斥锁，但是并发性能就会下降
        //使用 pread ，读取时完全不改变文件的全局偏移指针，同时并发读取文件，实现无锁竞争的高并发读取
        ssize_t n = pread(fd,buf+done,len-done,offset + static_cast<off_t>(done));

        if(n < 0)
        {   
            //被信号中断
            if(errno == EINTR)
            {
                continue;
            }

            //真错误
            return false;
        }

        //如果文件提前结束 EOF 拦截
        if(n == 0)
        {
            return false;
        }

        done += static_cast<size_t>(n);
    }

    return true;

}




namespace kv
{

    enum class ScanStatus
    {
        Ok,
        Incomplete, //只发生在文件尾 安全截断
        Corrupt, //完整记录损坏 拒绝启动
        IoError //pread 等系统调用失败
    };


    //读取 offset 处一条记录
    static ScanStatus scan_record_at(int fd,off_t file_size,off_t offset,Record &out,size_t &consumed)
    {
        consumed = 0;

        if(offset < 0 || offset > file_size)
        {
            return ScanStatus::Corrupt;
        }

        off_t remaining = file_size - offset;

        //不足固定头部长度
        if(remaining < static_cast<off_t>(RECORD_HEADER_SIZE))
        {
            return ScanStatus::Incomplete;
        }

        char hdr[RECORD_HEADER_SIZE];

        if(!::read_exact(fd,hdr,sizeof(hdr),offset))
        {
            //pread 失败则 IO 错 不是普通的残损尾部
            return ScanStatus::IoError;
        }

        //验证头校验和
        if(crc32(hdr+RECORD_PAYLOAD_CRC_OFF,RECORD_HEADER_SIZE-RECORD_HEADER_CRC_SIZE) != read_be32(hdr))
        {
            return ScanStatus::Corrupt;
        }


        uint8_t type = static_cast<uint8_t>(hdr[RECORD_TYPE_OFF]);
        uint32_t klen = read_be32(hdr+RECORD_KEY_LEN_OFF);
        uint32_t vlen = read_be32(hdr+RECORD_VAL_LEN_OFF);

        //完整头里的元数据非法
        if(type != static_cast<uint8_t>(RecordType::Set) && type != static_cast<uint8_t>(RecordType::Del))
        {
            return ScanStatus::Corrupt;
        }

        if(klen == 0 || klen > MAX_KEY || vlen > MAX_VALUE)
        {
            return ScanStatus::Corrupt;
        }

        //墓碑无 value
        if(type == static_cast<uint8_t>(RecordType::Del) && vlen != 0)
        {
            return ScanStatus::Corrupt;
        }

        size_t total = RECORD_HEADER_SIZE + static_cast<size_t>(klen) + static_cast<size_t>(vlen);

        if(remaining < static_cast<off_t>(total))
        {
            return ScanStatus::Incomplete;
        }

        std::vector<char>bytes(total);

        if(! :: read_exact(fd,bytes.data(),bytes.size(),offset))
        {
            return ScanStatus::IoError;
        }

        if(!decode_record(bytes.data(),bytes.size(),out,consumed))
        {
            return ScanStatus::Corrupt;
        }

        if(consumed != total || out.key.empty())
        {
            return ScanStatus::Corrupt;
        }

        return ScanStatus::Ok;

    }



    
    Storage::Storage(const std::string &path,OpenMode mode)
    {   
        int flags = O_RDWR | O_CREAT | O_CLOEXEC;

        if(mode == OpenMode::Rebuild)
        {
            flags |= O_NOFOLLOW | O_NONBLOCK;
        }

        fd_ = open(path.c_str(),flags,mode == OpenMode::Rebuild ? 0600 : 0644);

        if(fd_ < 0)
        {
            std::cerr << "open " << path << " failed: " << strerror(errno) << std::endl;
            return; //未打开成功 fd_ 仍等于 -1 good() == false
        }

        if(mode == OpenMode::Rebuild)
        {
            struct stat info{};
            const int result = fstat(fd_,&info);

            if(result < 0 || !S_ISREG(info.st_mode) || info.st_nlink != 1) 
            {
                const int code = errno;
                std::cerr << "rebuild target check failed : " << path << " : " << (result < 0 ? strerror(code) : "expected a regular file with one link") << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }
        }

        //文件独占锁
        if(flock(fd_,LOCK_EX | LOCK_NB) != 0) //LOCK_NB 拿不到就直接失败
        {
            std::cerr << "database is locked : " << path << " ( " << strerror(errno) << " ) " << std::endl;
            close(fd_);
            fd_ = -1;
            return;
        }

        //仅重建派生库
        if(mode == OpenMode::Rebuild)
        {
            if(ftruncate(fd_,0) != 0)
            {
                std::cerr << "rebuild truncate failed : " << strerror(errno) << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }
        }

        //lseek 获取文件大小
        off_t size = lseek(fd_,0,SEEK_END);

        if(size < 0)
        {
            std::cerr << "lseek failed" << std::endl;
            close(fd_);
            fd_ = -1;
            return;
        }

        //即新建的全新文件
        if(size == 0)
        {
            //显式构造一个名为 header 的字符串对象，精准复制 MAGIC
            std::string header(MAGIC,sizeof(MAGIC));
            append_be32(header,FORMAT_VERSION);

            //如果 write_all 成功了，调用 fsync 将这 8 字节强制刷到磁盘中
            if(!write_all(fd_,header.data(),header.size()) || fsync(fd_) != 0)
            {
                std::cerr << "write file header failde" << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }

        }
        else //已有文件 校验头
        {
            if(size < static_cast<off_t>(FILE_HEADER_SIZE))
            {
                std::cerr << "file too small to be a kv data file" << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }

            char hdr[FILE_HEADER_SIZE];
            if(!read_exact(fd_,hdr,sizeof(hdr),0))
            {
                std::cerr << "read file header failed" << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }


            if(memcmp(hdr,MAGIC,sizeof(MAGIC)) != 0)
            {
                std::cerr << "not a kv data file (bad magic)" << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }

            uint32_t ver = read_be32(hdr+4);
            if(ver != FORMAT_VERSION)
            {
                std::cerr << "unsupported format version : " << ver << std::endl;
                close(fd_);
                fd_ = -1;
                return;
            }

            //文件头校验完成后

            if(!recover())
            {
                close(fd_);
                fd_ = -1;
                return;
            }


        }


    }



    Storage::~Storage()
    {
        if(fd_ >= 0)
        {
            close(fd_);
        }
    }


    bool Storage::put(const std::string &key,const std::string &value)
    {
        return put_deferred(key,value) && flush();
    }


    void Storage::dump_index(std::ostream &os) const
    {
        for(const auto &kv : index_)
        {
            os << " " << kv.first << " -> offset = " << kv.second.offset << " len = " << kv.second.total_len << "\n";
        }
    }



    off_t Storage::append_record(const std::string &rec)
    {
        //已不可信
        if(poisoned_)
        {
            std::cerr << "append_record: storage is poisoned,reopen required" << std::endl;
            return -1;
        }

        //取得当前位置的偏移
        off_t offset = lseek(fd_,0,SEEK_END);

        if(offset < 0)
        {
            std::cerr << "lseek failed : " << strerror(errno) << std::endl;
            poisoned_ = true;
            return -1;
        }

        if(!write_all(fd_,rec.data(),rec.size()))
        {
            //如果写坏 将文件截回去
            if(ftruncate(fd_,offset) != 0)
            {
                std::cerr << "ftruncate rollback failed : " << strerror(errno) << std::endl;
                poisoned_ = true;
            }

            return -1;
        }


        return offset;

    }


    bool Storage::get(const std::string &key,std::string &value)
    {
        auto it = index_.find(key);

        if(it == index_.end())
        {
            return false; //没找到
        }

        const IndexEntry &ent = it->second;

        //将记录读出来
        std::vector<char>buf(ent.total_len);
        if(!read_exact(fd_,buf.data(),buf.size(),static_cast<off_t>(ent.offset)))
        {
            return false;
        }

        Record rec;
        size_t consumed = 0;

        if(!decode_record(buf.data(),buf.size(),rec,consumed))
        {
            return false;
        }

        //墓碑记录不应该出现在索引中
        if(rec.key != key || rec.type != RecordType::Set)
        {
            return false;
        }

        value = rec.value;
        return true;


    }


    bool Storage::del(const std::string &key)
    {
        return del_deferred(key) && flush();

    }


    bool Storage::recover()
    {
        if(fd_ < 0)
        {
            return false;
        }

        off_t file_size = lseek(fd_,0,SEEK_END);

        if(file_size < static_cast<off_t>(FILE_HEADER_SIZE))
        {
            std::cerr<< "recover: file too small" << std::endl;
            return false;
        }

        //建立临时表 完整恢复再替换 index_(keydir)
        std::unordered_map<std::string,IndexEntry>rebuilt;

        off_t offset = static_cast<off_t>(FILE_HEADER_SIZE);

        while(offset < file_size)
        {
            Record rec;
            size_t consumed = 0;

            ScanStatus status = scan_record_at(fd_,file_size,offset,rec,consumed);

            if(status == ScanStatus::Incomplete)
            {
                std::cerr << "recover : truncate incomplete tail at offset = " << offset << std::endl;

                //截掉 残损尾部
                if(ftruncate(fd_,offset) != 0)
                {
                    std::cerr << "recover : ftruncate failed : " << strerror(errno) << std::endl;

                    return false;
                }
                

                if(fsync(fd_) != 0)
                {
                    std::cerr << "recover : fsync after truncate failed : " << strerror(errno) << std::endl;

                    return false;
                }

                break;

            }

            if(status == ScanStatus::Corrupt)
            {
                std::cerr << "recover : corrupt record at offset = " << offset << std::endl;

                return false;
            }

            if(status == ScanStatus::IoError)
            {
                std::cerr << "recover : IO error at offset = " << std::endl;

                return false;
            }

            //如果 status == ScanStatus:: OK
            //按照文件先后顺序重放
            if(rec.type == RecordType::Set)
            {
                rebuilt[rec.key] = IndexEntry{static_cast<uint64_t>(offset),
                static_cast<uint32_t>(consumed)};
            }
            else //墓碑标识
            {
                rebuilt.erase(rec.key);
            }

            offset += static_cast<off_t>(consumed);

        }

        index_.swap(rebuilt);

        return true;

    }

    bool Storage::put_deferred(const std::string &key,const std::string &value)
    {
        //合法性检查
        if(key.empty() || key.size() > MAX_KEY || value.size() > MAX_VALUE)
        {
            return false;
        }

        //编码 转换为 SET 记录
        std::string rec = encode_record(RecordType::Set,key,value);

        //记录落在文件末尾，先取当前位置作为偏移 (追加)
        off_t offset = append_record(rec);

        if(offset < 0)
        {
            return false;
        }

        //更新内存索引 指向最新
        index_[key] = IndexEntry{static_cast<uint64_t>(offset),static_cast<uint32_t>(rec.size())};

        return true;
    }


    bool Storage::del_deferred(const std::string &key)
    {
        //非法 key
        if(key.empty() || key.size() > MAX_KEY)
        {
            return false;
        }

        //不存在 key 不追加无意义墓碑
        auto it = index_.find(key);
        if(it == index_.end())
        {
            return false;
        }

        //DEL 记录没有 value
        std::string rec = encode_record(RecordType::Del,key,"");

        off_t offset = append_record(rec);

        if(offset < 0)
        {
            return false;
        }

        index_.erase(it);

        return true;
    }


    bool Storage::flush()
    {
        if(!sync_on_put_)
        {
            return true;
        }

        if(fsync(fd_) != 0)
        {
            std::cerr << "fsync failed : " << strerror(errno) << std::endl;
            poisoned_ = true;
            return false;
        }

        return true;
    }





}//namespace kv
