#ifndef REACTOR_H
#define REACTOR_H

class Reactor
{
public:
    Reactor();
    ~Reactor();
    void loop();

private:
    int epfd_;    
};

#endif