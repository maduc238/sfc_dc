"""Cooperative wall-clock deadline shared by preprocessing, sampling and validation."""

import time


class Budget:
    def __init__(self, milliseconds, clock=time.perf_counter):
        self.clock = clock
        self.started = clock()
        self.deadline = self.started + milliseconds / 1000

    @property
    def remaining_ms(self):
        return max(0.0, (self.deadline - self.clock()) * 1000)

    @property
    def expired(self):
        return self.remaining_ms <= 0

    def check(self):
        if self.expired:
            raise TimeoutError('Decision budget exhausted')
