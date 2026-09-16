// test_noncopyable.cpp —— Connection / Reactor 的"不可拷贝、不可移动"类型契约测试（Step 7）
//
// 【这个测试在验证什么】
//   这个文件里没有运行时逻辑，验证全部发生在【编译期】：
//   用 std::is_*_constructible / std::is_*_assignable 检查四个特殊成员函数都不可用。
//   只要有人删掉 = delete（让某个操作重新变得可用），这里就编译失败，run_all.sh 的构建步骤直接红。
//   注意它的边界：它验证的是【契约】（四种操作不可用），不是【声明的拼写】——
//   把 private 拷贝构造写成别的重载、只要结果仍然不可用，这里照样过；拼写正确性只能靠 review。
//
// 【为什么这两个类必须禁止拷贝和移动】
//   · Connection 的析构函数会 close(fd_)，而 epoll 的 data.ptr 指向它的稳定地址：
//       复制 → 两个对象持有同一个 fd → 双重 close，甚至误关已被内核复用的新连接
//       移动 → data.ptr 仍指向旧对象，事件回来时指向失效对象
//   · Reactor 独占 epoll 实例和全部 Connection（unique_ptr 集合）：
//       复制/移动 → epoll fd 所有权不清、Connection::reactor_ 回指悬空
//   · 需要转移所有权时，移动的是 std::unique_ptr<Connection>（所有权），
//     不是 Connection 对象本身（地址必须稳定）—— 见下面的"正向对照"。
//
// 【用法】
//   cmake --build build -j && ./build/test_noncopyable
#include "net/connection.h"
#include "net/reactor.h"
#include <memory>
#include <type_traits>
#include <iostream>

// ---------- 反向断言：四种操作统统不可用 ----------

// Connection：持有 socket fd
static_assert(!std::is_copy_constructible_v<Connection>, "Connection 不能拷贝构造");
static_assert(!std::is_copy_assignable_v<Connection>,    "Connection 不能拷贝赋值");
static_assert(!std::is_move_constructible_v<Connection>, "Connection 不能移动构造");
static_assert(!std::is_move_assignable_v<Connection>,    "Connection 不能移动赋值");

// Reactor：持有 epoll fd + 全部连接的所有权
static_assert(!std::is_copy_constructible_v<Reactor>, "Reactor 不能拷贝构造");
static_assert(!std::is_copy_assignable_v<Reactor>,    "Reactor 不能拷贝赋值");
static_assert(!std::is_move_constructible_v<Reactor>, "Reactor 不能移动构造");
static_assert(!std::is_move_assignable_v<Reactor>,    "Reactor 不能移动赋值");

// 两个类都必须是可析构的（RAII 的前提，别把析构也一起禁掉）
static_assert(std::is_destructible_v<Connection>, "Connection 必须可析构（RAII 关闭 fd）");
static_assert(std::is_destructible_v<Reactor>,    "Reactor 必须可析构（RAII 关闭 epfd）");

// ---------- 正向对照：所有权可以通过 unique_ptr 转移 ----------
// 这是本设计里唯一允许的"移动"：搬的是所有权，不是对象地址。

static_assert(std::is_move_constructible_v<std::unique_ptr<Connection>>,
              "unique_ptr<Connection> 必须可移动构造（add_connection 的所有权转移依赖它）");
static_assert(std::is_move_assignable_v<std::unique_ptr<Connection>>,
              "unique_ptr<Connection> 必须可移动赋值（connections_[fd] = std::move(...) 依赖它）");
static_assert(!std::is_copy_constructible_v<std::unique_ptr<Connection>>,
              "unique_ptr 不能拷贝（所有权唯一）");

int main()
{
    std::cout << "noncopyable : PASS（Connection / Reactor 不可拷贝、不可移动；"
                 "所有权只能经 unique_ptr 转移）" << std::endl;
    return 0;
}
