#include "net/connection.h"


Connection::~Connection()
{
    close(fd_);
}