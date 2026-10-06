# pairwise-lockmgr

一个纯内存的数据库锁管理器内核：共享 / 排他 / 意向锁共用一张兼容矩阵，支持同一事务的
模式合并、加锁与升级、严格先进先出的等待队列、等待图死锁检测与逻辑超时。
只用 Python 标准库，不连接真实数据库、不联网，时间由调用方注入的逻辑时钟推进。

## 目录

- `lockmgr/core.py` — 内核：兼容矩阵、锁表、等待队列、死锁检测、超时
- `tests/test_core.py` — 内核的行为测试

## 怎么跑测试

在项目根目录执行：

    python3 -m unittest discover -s tests -v

Windows 上把 `python3` 换成解释器路径，例如：

    C:/Users/<你>/AppData/Local/Programs/Python/Python313/python.exe -m unittest discover -s tests -v
