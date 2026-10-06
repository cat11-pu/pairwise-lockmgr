"""数据库锁管理器内核（纯内存、确定性，只用 Python 标准库）。

资源是不透明字符串（表、页、行都行），每个资源上可以授予 IS / IX / S / SIX / X 五种
模式，两两是否冲突查同一张兼容矩阵。同一个事务在同一个资源上的多次加锁会按语义合并
成一个聚合模式：先持 S 再申请 IX 得到 SIX，先持 X 再申请 S 仍然是 X，所以重复申请
已经持有的同等或更弱模式是幂等的空操作。

行为约定
--------
* 等待队列严格先进先出：只有队首的请求会被授予，新来的请求即使与当前持有者兼容，
  只要队列里还有等待者就必须排在队尾，以免先到的等待者被不断到来的请求饿死；
* 等待图：一个等待请求被「与自己不兼容的持有者」和「排在自己前面的等待者」阻塞，
  加锁时如果这条新等待边会让等待图出现环（环可以跨多个事务），抛 DeadlockError，
  该请求不入队，事务已经拿到的锁保持不动；
* 升级：持有低级别锁的事务申请高级别锁而暂时无法授予时，事务继续持有原来的锁并
  排队等待，释放、降级、超时都会推动队列前进；
* 超时：timeout 以逻辑时钟的 tick 计，deadline 当刻即到期，到期请求从队列里作废，
  并出现在 advance() 的返回值与 expired() 里；
* 释放：release_all() 按加锁的逆序释放，先拿到的意向锁最后放。
"""

MODES = ("IS", "IX", "S", "SIX", "X")

# 兼容矩阵：_COMPATIBLE[(a, b)] 表示 a、b 能否被不同事务同时持有。
_COMPATIBLE = {
    ("IS", "IS"): True, ("IS", "IX"): True, ("IS", "S"): True,
    ("IS", "SIX"): True, ("IS", "X"): False,
    ("IX", "IS"): True, ("IX", "IX"): True, ("IX", "S"): False,
    ("IX", "SIX"): False, ("IX", "X"): False,
    ("S", "IS"): True, ("S", "IX"): False, ("S", "S"): True,
    ("S", "SIX"): False, ("S", "X"): False,
    ("SIX", "IS"): False, ("SIX", "IX"): False, ("SIX", "S"): False,
    ("SIX", "SIX"): False, ("SIX", "X"): False,
    ("X", "IS"): False, ("X", "IX"): False, ("X", "S"): False,
    ("X", "SIX"): False, ("X", "X"): False,
}

# 聚合矩阵：同一事务先后申请 a、b 之后最终持有的模式，两个方向一致。
_JOIN = {}
for _first, _second, _merged in (
    ("IS", "IS", "IS"), ("IS", "IX", "IX"), ("IS", "S", "S"),
    ("IS", "SIX", "SIX"), ("IS", "X", "X"),
    ("IX", "IX", "IX"), ("IX", "S", "SIX"), ("IX", "SIX", "SIX"),
    ("IX", "X", "X"),
    ("S", "S", "S"), ("S", "SIX", "SIX"), ("S", "X", "X"),
    ("SIX", "SIX", "SIX"), ("SIX", "X", "X"),
    ("X", "X", "X"),
):
    _JOIN[(_first, _second)] = _merged
    _JOIN[(_second, _first)] = _merged


def compatible(a, b):
    """两种模式能否被不同事务同时持有。"""
    return _COMPATIBLE[(a, b)]


def join(a, b):
    """同一事务先后申请 a、b 之后最终应当持有的模式。"""
    return _JOIN[(a, b)]


def _key(value):
    """事务名与资源名统一成字符串，避免同一个对象出现两种键。"""
    return value if isinstance(value, str) else str(value)


class Clock:
    """可注入的逻辑时钟：只由调用方推进，内核不读真实时间。"""

    def __init__(self, start=0):
        self._now = int(start)

    def now(self):
        return self._now

    def advance(self, ticks=1):
        self._now += int(ticks)
        return self._now


class LockError(Exception):
    """锁管理器的用法错误。"""


class DeadlockError(LockError):
    """这次加锁会让等待图出现环。"""


class _Waiter:
    """等待队列里的一个请求。"""

    __slots__ = ("txn", "mode", "deadline", "seq")

    def __init__(self, txn, mode, deadline, seq):
        self.txn = txn
        self.mode = mode
        self.deadline = deadline
        self.seq = seq


class LockManager:
    """多粒度锁管理器。

    参数：
        clock  可注入的逻辑时钟，缺省新建一个；等待请求的 timeout 以它的 tick 计。
    """

    def __init__(self, clock=None):
        self.clock = clock if clock is not None else Clock()
        self._holders = {}      # 资源 -> {事务: 当前模式}
        self._queues = {}       # 资源 -> [等待中的请求]
        self._txn_locks = {}    # 事务 -> 按加锁顺序排列的资源
        self._expired = []      # 累计被超时作废的请求
        self._seq = 0           # 等待请求的到达序号

    # ------------------------------------------------------------- 加锁

    def acquire(self, txn, resource, mode, timeout=None):
        """申请（或升级到）某个模式。

        立即拿到锁时返回 True；需要等待时返回 False，请求按到达顺序进入该资源的等待
        队列，之后每次 release / downgrade / advance 都会尝试授予队首。已经持有同等
        或更强模式时是幂等的空操作；等待中的请求再次申请会被合并，不会排两次队。
        如果这次等待会让等待图成环，抛 DeadlockError，请求不入队。
        """
        txn = _key(txn)
        resource = _key(resource)
        if mode not in MODES:
            raise LockError("未知的锁模式 %r" % (mode,))
        held = self._holders.get(resource, {}).get(txn)
        pending = self._pending(resource, txn)
        if pending is not None:
            pending.mode = join(pending.mode, mode)
            return False
        target = mode
        if target == held:
            return True
        if self._must_wait(resource, txn, target):
            blockers = sorted(self._blockers(resource, txn, target))
            if self._creates_cycle(txn, blockers):
                raise DeadlockError(
                    "事务 %s 在资源 %s 上申请 %s 会让等待图成环，阻塞者：%s"
                    % (txn, resource, target, ", ".join(blockers))
                )
            if held is not None:
                # 转换请求挂起期间先摘下旧模式，等队首轮到时再重新授予。
                self._holders[resource].pop(txn)
            self._enqueue(resource, txn, target, timeout)
            return False
        self._set_holder(resource, txn, target)
        return True

    def release(self, txn, resource):
        """释放某个资源上的锁，并作废该事务在同一资源上的等待请求。"""
        txn = _key(txn)
        resource = _key(resource)
        holders = self._holders.get(resource, {})
        if txn not in holders:
            raise LockError("事务 %s 没有持有资源 %s 上的锁" % (txn, resource))
        mode = holders.pop(txn)
        self._drop_pending(resource, txn)
        locks = self._txn_locks.get(txn)
        if locks and resource in locks:
            locks.remove(resource)
        self._dispatch(resource)
        return mode

    def release_all(self, txn):
        """按加锁的逆序释放该事务的全部锁，返回实际释放的资源顺序。"""
        txn = _key(txn)
        held = list(self._txn_locks.get(txn, ()))
        released = []
        for resource in held:
            holders = self._holders.get(resource, {})
            if txn in holders:
                holders.pop(txn)
            self._drop_pending(resource, txn)
            self._dispatch(resource)
            released.append(resource)
        self._txn_locks[txn] = []
        return released

    def downgrade(self, txn, resource, new_mode):
        """把持有的锁降到更低级别；只允许降级，不允许升级或横向变更。"""
        txn = _key(txn)
        resource = _key(resource)
        if new_mode not in MODES:
            raise LockError("未知的锁模式 %r" % (new_mode,))
        held = self._holders.get(resource, {}).get(txn)
        if held is None:
            raise LockError("事务 %s 没有持有资源 %s 上的锁" % (txn, resource))
        if join(held, new_mode) != held:
            raise LockError("%s 不是 %s 的降级目标" % (new_mode, held))
        self._holders[resource][txn] = new_mode
        return new_mode

    def advance(self, ticks=1):
        """推进逻辑时钟，把到期的等待请求作废并返回它们。"""
        self.clock.advance(ticks)
        now = self.clock.now()
        expired = []
        for resource in sorted(self._queues):
            queue = self._queues[resource]
            kept = []
            for waiter in queue:
                if waiter.deadline is not None and waiter.deadline < now:
                    expired.append((waiter.txn, resource, waiter.mode))
                else:
                    kept.append(waiter)
            if len(kept) != len(queue):
                self._queues[resource] = kept
                self._dispatch(resource)
        self._expired.extend(expired)
        return expired

    # ------------------------------------------------------------- 观察

    def holder_mode(self, txn, resource):
        """该事务在该资源上当前持有的模式，没有则返回 None。"""
        return self._holders.get(_key(resource), {}).get(_key(txn))

    def holders(self, resource):
        """该资源当前的持有者 {事务: 模式}，按事务名排序便于比较。"""
        current = self._holders.get(_key(resource), {})
        return {txn: current[txn] for txn in sorted(current)}

    def waiting(self, resource):
        """该资源的等待队列，按到达顺序返回 [(事务, 模式), ...]。"""
        return [(waiter.txn, waiter.mode) for waiter in self._queues.get(_key(resource), ())]

    def locks_of(self, txn):
        """该事务按加锁顺序排列的资源列表。"""
        return list(self._txn_locks.get(_key(txn), ()))

    def expired(self):
        """累计被超时作废的等待请求，[(事务, 资源, 模式), ...]。"""
        return list(self._expired)

    # ------------------------------------------------------------- 内部

    def _pending(self, resource, txn):
        """该事务在该资源上排队中的请求。"""
        for waiter in self._queues.get(resource, ()):
            if waiter.txn == txn:
                return waiter
        return None

    def _drop_pending(self, resource, txn):
        """撤销该事务在该资源上的等待请求。"""
        queue = self._queues.get(resource)
        if not queue:
            return False
        kept = [waiter for waiter in queue if waiter.txn != txn]
        if len(kept) == len(queue):
            return False
        self._queues[resource] = kept
        return True

    def _can_grant(self, resource, txn, mode):
        """当前持有者是否都容得下这个请求（不看队列）。"""
        for holder, held in self._holders.get(resource, {}).items():
            if holder == txn:
                continue
            if not compatible(held, mode):
                return False
        return True

    def _blockers(self, resource, txn, mode):
        """阻塞这个请求的事务集合。"""
        blockers = set()
        for holder, held in self._holders.get(resource, {}).items():
            if holder != txn and not compatible(held, mode):
                blockers.add(holder)
        return blockers

    def _must_wait(self, resource, txn, mode):
        """当前持有者容不下这个请求时就要排队。"""
        return not self._can_grant(resource, txn, mode)

    def _wait_edges(self):
        """等待图：事务 -> 它正在等待的事务。"""
        edges = {}
        for resource, queue in self._queues.items():
            for waiter in queue:
                edges.setdefault(waiter.txn, set()).update(
                    self._blockers(resource, waiter.txn, waiter.mode)
                )
        return edges

    def _creates_cycle(self, txn, blockers):
        """假设 txn 新增这些等待边，判断等待图里是否出现经过 txn 的环。"""
        edges = self._wait_edges()
        for blocker in blockers:
            if txn in edges.get(blocker, ()):
                return True
        return False

    def _enqueue(self, resource, txn, mode, timeout):
        """把请求按到达顺序放进等待队列。"""
        self._seq += 1
        deadline = None if timeout is None else self.clock.now() + int(timeout)
        self._queues.setdefault(resource, []).append(
            _Waiter(txn, mode, deadline, self._seq)
        )

    def _set_holder(self, resource, txn, mode):
        """记录持有关系与加锁顺序。"""
        self._holders.setdefault(resource, {})[txn] = mode
        locks = self._txn_locks.setdefault(txn, [])
        if resource not in locks:
            locks.append(resource)

    def _dispatch(self, resource):
        """从队首开始尝试授予，队首授不下去时后面的请求一律不许越过。"""
        queue = self._queues.get(resource)
        if not queue:
            return
        granted = []
        remaining = []
        for waiter in queue:
            if not remaining and self._can_grant(resource, waiter.txn, waiter.mode):
                self._set_holder(resource, waiter.txn, waiter.mode)
                granted.append((waiter.txn, waiter.mode))
            else:
                remaining.append(waiter)
        self._queues[resource] = remaining
        return granted
