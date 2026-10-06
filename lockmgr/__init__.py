"""lockmgr：纯内存的数据库锁管理器内核。

对外入口：
    LockManager   锁管理器：兼容矩阵、模式合并、等待队列、死锁检测与超时
    Clock         可注入的逻辑时钟，等待请求的 timeout 以它的 tick 计
    compatible    两种模式能否被不同事务同时持有
    join          同一事务先后申请两种模式之后最终持有的模式
    LockError     用法错误
    DeadlockError 这次加锁会让等待图成环
"""

from .core import (
    MODES,
    Clock,
    DeadlockError,
    LockError,
    LockManager,
    compatible,
    join,
)

__all__ = [
    "MODES",
    "Clock",
    "DeadlockError",
    "LockError",
    "LockManager",
    "compatible",
    "join",
]
