#include "raft/apply.h"

namespace raft
{
    bool apply_append_entries(RaftCore& core,StateStore& state,LogStore& store,const AppendEntriesOut& out,AppendEntriesResponse& response,std::string& error)
    {
        error.clear();

        response.term = out.term;
        response.success = false;

        //任期变了就先落盘
        if(out.hard_state.has_value())
        {
            if(!state.save(*out.hard_state,error))
            {
                core.stop();
                return false;
            }
        }

        if(!out.accept)
        {
            return true;
        }

        //删掉冲突后缀
        if(out.truncate_from != 0)
        {
            if(!store.truncate_suffix(out.truncate_from,error))
            {
                core.stop();
                return false;
            }
        }

        if(!out.entries.empty())
        {
            if(!store.append_batch(out.entries,error))
            {
                core.stop();
                return false;
            }
        }

        if(!core.on_log_persisted(error))
        {
            core.stop();
            return false;
        }

        response.success = true;
        return true;

    }
}//namespace raft