import random
import functools
from node import Node
from heapq import heappush, heappop


class Network(object):
    PROP_DELAY = 0.03
    PROP_JITTER = 0.02
    DROP_PROB = 0.05

    def __init__(self, seed):
        self.nodes = {}
        self.rnd = random.Random(seed)
        self.timers = []
        self.now = 1000.0

    def new_node(self, address=None):
        node = Node(self, address=address)
        self.nodes[node.address] = node
        return node

    def run(self):
        while self.timers:
            next_timer = self.timers[0]
            if next_timer.expires > self.now:
                self.now = next_timer.expires
            heappop(self.timers)
            if next_timer.cancelled:
                continue
            if not next_timer.address or next_timer.address in self.nodes:
                next_timer.callback()

    def stop(self):
        self.timers = []

    def set_timer(self, address, seconds, callback):
        timer = Timer(self.now + seconds, address, callback)
        heappush(self.timers, timer)
        return timer

    def send(self, sender, destinations, message):
        sender.logger.debug('sending %s to %s', message, destinations)
        for dest in (d for d in destinations if d in self.nodes):
            if dest == sender.address:
                self.set_timer(sender.address, 0, lambda: sender.receive(sender.address, message))
            elif self.rnd.uniform(0, 1.0) > self.DROP_PROB:
                delay = self.PROP_DELAY + self.rnd.uniform(-self.PROP_JITTER, self.PROP_JITTER)
                self.set_timer(
                    dest, delay, functools.partial(
                        self.nodes[dest].receive,
                        sender.address,
                        message
                    )
                )


class Timer(object):
    def __init__(self, expires, address, callback):
        self.expires = expires
        self.address = address
        self.callback = callback
        self.cancelled = False

    # def __cmp__(self, other):
    #     if self.expires < other.expires:
    #         return -1
    #     elif self.expires == other.expires:
    #         return 0
    #     else:
    #         return 1

    def __lt__(self, other):
        return self.expires < other.expires

    def __eq__(self, other):
        return self.expires == other.expires

    def cancel(self):
        self.cancelled = True
