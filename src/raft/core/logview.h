#ifndef RAFT_LOGVIEW_H
#define RAFT_LOGVIEW_H

#include "raft/core/core.h"
#include "raft/core/logstore.h"
#include <string>

namespace raft
{

class LogStoreView:public LogView
{
public:

explicit LogStoreView(LogStore& store):store_(&store)
{

}

std::uint64_t last_index()const override
{
    return store_->last_index();
}

bool term_at(std::uint64_t index,std::uint64_t& out)const override
{
    std::string error;
    return store_->term_at(index,out,error);
}

bool entry_at(std::uint64_t index,LogEntry& out)const override
{
    std::string error;
    return store_->get(index,out,error);
}

private:

LogStore* store_;

};




}//namespace raft



#endif