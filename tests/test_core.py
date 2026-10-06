"""lockmgr.core 的行为测试：兼容矩阵、加锁与升级、等待队列、死锁与超时。"""

import unittest

from lockmgr.core import Clock, DeadlockError, LockError, LockManager


class LockManagerCoreTest(unittest.TestCase):
    """覆盖正常路径、边界输入、异常路径与不变量。"""

    def setUp(self):
        self.clock = Clock()
        self.mgr = LockManager(clock=self.clock)

    def test_shared_holders_coexist_and_writers_wait_their_turn(self):
        """共享锁可以共存，排他锁要等到所有共享持有者释放。"""
        self.assertTrue(self.mgr.acquire("t1", "page:1", "S"))
        self.assertTrue(self.mgr.acquire("t2", "page:1", "S"))
        self.assertEqual(self.mgr.holders("page:1"), {"t1": "S", "t2": "S"})
        self.assertFalse(self.mgr.acquire("t3", "page:1", "X"))
        self.assertIsNone(self.mgr.holder_mode("t3", "page:1"))
        self.assertEqual(self.mgr.waiting("page:1"), [("t3", "X")])
        self.mgr.release("t1", "page:1")
        self.assertIsNone(self.mgr.holder_mode("t3", "page:1"))
        self.mgr.release("t2", "page:1")
        self.assertEqual(self.mgr.holder_mode("t3", "page:1"), "X")

    def test_intent_modes_merge_and_follow_the_compatibility_matrix(self):
        """同一事务的模式按语义合并，不同事务之间按兼容矩阵判定。"""
        self.assertTrue(self.mgr.acquire("t1", "db.t", "S"))
        self.assertTrue(self.mgr.acquire("t1", "db.t", "IX"))
        self.assertEqual(self.mgr.holder_mode("t1", "db.t"), "SIX")
        self.assertTrue(self.mgr.acquire("t2", "db.t", "IS"))
        self.assertEqual(self.mgr.holders("db.t"), {"t1": "SIX", "t2": "IS"})
        self.assertFalse(self.mgr.acquire("t3", "db.t", "S"))
        self.assertEqual(self.mgr.waiting("db.t"), [("t3", "S")])

    def test_repeated_acquisition_is_idempotent_and_never_downgrades(self):
        """重复申请已持有的模式是空操作，申请更弱的模式不得把锁降下来。"""
        self.assertTrue(self.mgr.acquire("t1", "row:7", "X"))
        self.assertTrue(self.mgr.acquire("t1", "row:7", "X"))
        self.assertTrue(self.mgr.acquire("t1", "row:7", "S"))
        self.assertEqual(self.mgr.holder_mode("t1", "row:7"), "X")
        self.assertEqual(self.mgr.waiting("row:7"), [])
        self.assertFalse(self.mgr.acquire("t2", "row:7", "X"))
        self.assertFalse(self.mgr.acquire("t2", "row:7", "X"))
        self.assertEqual(self.mgr.waiting("row:7"), [("t2", "X")])
        self.mgr.release("t1", "row:7")
        self.assertEqual(self.mgr.holder_mode("t2", "row:7"), "X")
        self.assertEqual(self.mgr.locks_of("t1"), [])

    def test_blocked_upgrade_keeps_the_lower_lock(self):
        """升级暂时拿不到时，事务继续持有原来的锁，并在对手释放后拿到升级。"""
        self.assertTrue(self.mgr.acquire("t1", "page:3", "S"))
        self.assertTrue(self.mgr.acquire("t2", "page:3", "S"))
        self.assertFalse(self.mgr.acquire("t1", "page:3", "X"))
        self.assertEqual(self.mgr.holder_mode("t1", "page:3"), "S")
        self.assertEqual(self.mgr.holders("page:3"), {"t1": "S", "t2": "S"})
        self.assertEqual(self.mgr.waiting("page:3"), [("t1", "X")])
        self.mgr.release("t2", "page:3")
        self.assertEqual(self.mgr.holder_mode("t1", "page:3"), "X")

    def test_queued_writer_turns_a_later_upgrade_into_a_deadlock(self):
        """已经有人在排队等写锁时，持有者再升级就构成死锁，必须报错而不是干等。"""
        self.assertTrue(self.mgr.acquire("t1", "page:5", "S"))
        self.assertFalse(self.mgr.acquire("t2", "page:5", "X"))
        with self.assertRaises(DeadlockError):
            self.mgr.acquire("t1", "page:5", "X")
        self.assertEqual(self.mgr.holder_mode("t1", "page:5"), "S")
        self.assertEqual(self.mgr.waiting("page:5"), [("t2", "X")])

    def test_cycle_across_three_transactions_is_reported(self):
        """等待环跨三个事务时同样必须被判定为死锁。"""
        self.assertTrue(self.mgr.acquire("t1", "r1", "S"))
        self.assertTrue(self.mgr.acquire("t2", "r2", "S"))
        self.assertTrue(self.mgr.acquire("t3", "r3", "S"))
        self.assertFalse(self.mgr.acquire("t1", "r2", "X"))
        self.assertFalse(self.mgr.acquire("t2", "r3", "X"))
        with self.assertRaises(DeadlockError):
            self.mgr.acquire("t3", "r1", "X")
        self.assertEqual(self.mgr.holder_mode("t3", "r3"), "S")
        self.assertEqual(len(self.mgr.waiting("r1")), 0)

    def test_waiting_request_expires_at_its_deadline(self):
        """等待到逻辑时钟走到 deadline 时作废，并且不得再被授予。"""
        self.assertTrue(self.mgr.acquire("t1", "page:6", "X"))
        self.assertFalse(self.mgr.acquire("t2", "page:6", "X", timeout=5))
        self.assertFalse(self.mgr.acquire("t3", "page:6", "X"))
        self.assertEqual(self.mgr.waiting("page:6"), [("t2", "X"), ("t3", "X")])
        expired = self.mgr.advance(5)
        self.assertEqual(expired, [("t2", "page:6", "X")])
        self.assertEqual(self.mgr.waiting("page:6"), [("t3", "X")])
        self.mgr.release("t1", "page:6")
        self.assertEqual(self.mgr.holder_mode("t3", "page:6"), "X")
        self.assertIsNone(self.mgr.holder_mode("t2", "page:6"))

    def test_downgrade_grants_the_waiting_reader(self):
        """降级之后，等在后面的兼容请求要立刻被授予。"""
        self.assertTrue(self.mgr.acquire("t1", "page:7", "X"))
        self.assertFalse(self.mgr.acquire("t2", "page:7", "S"))
        self.assertEqual(self.mgr.holders("page:7"), {"t1": "X"})
        self.mgr.downgrade("t1", "page:7", "S")
        self.assertEqual(self.mgr.holder_mode("t1", "page:7"), "S")
        self.assertEqual(self.mgr.holder_mode("t2", "page:7"), "S")
        self.assertEqual(self.mgr.waiting("page:7"), [])
        with self.assertRaises(LockError):
            self.mgr.downgrade("t2", "page:7", "X")

    def test_later_request_does_not_barge_past_a_waiting_writer(self):
        """后来的请求即使与当前持有者兼容，也不得越过已经排队的等待者。"""
        self.assertTrue(self.mgr.acquire("t1", "page:8", "S"))
        self.assertFalse(self.mgr.acquire("t2", "page:8", "X"))
        self.assertFalse(self.mgr.acquire("t3", "page:8", "S"))
        self.assertIsNone(self.mgr.holder_mode("t3", "page:8"))
        self.assertEqual(self.mgr.waiting("page:8"), [("t2", "X"), ("t3", "S")])
        self.mgr.release("t1", "page:8")
        self.assertEqual(self.mgr.holder_mode("t2", "page:8"), "X")
        self.assertFalse(self.mgr.acquire("t4", "page:8", "S"))

    def test_release_all_releases_in_reverse_acquisition_order(self):
        """一次性释放按加锁的逆序进行，先拿到的意向锁最后放。"""
        self.assertTrue(self.mgr.acquire("t1", "r1", "IX"))
        self.assertTrue(self.mgr.acquire("t1", "r2", "IX"))
        self.assertTrue(self.mgr.acquire("t1", "r3", "X"))
        self.assertEqual(self.mgr.locks_of("t1"), ["r1", "r2", "r3"])
        self.assertEqual(self.mgr.release_all("t1"), ["r3", "r2", "r1"])
        self.assertEqual(self.mgr.locks_of("t1"), [])
        for resource in ("r1", "r2", "r3"):
            self.assertEqual(self.mgr.holders(resource), {})


if __name__ == "__main__":
    unittest.main()
