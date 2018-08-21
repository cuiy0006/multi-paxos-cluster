import itertools
import functools
import logging
from common import SimTimeLogger

# Acceptor -- make promises and accept proposals
# Replica -- manage the distributed state machine: submitting proposals, committing decisions, and responding to requesters
# Leader -- lead rounds of the Multi-Paxos algorithm
# Scout -- perform the Prepare/Promise portion of the Multi-Paxos algorithm for a leader
# Commander -- perform the Accept/Accepted portion of the Multi-Paxos algorithm for a leader
# Bootstrap -- introduce a new node to an existing cluster
# Seed -- create a new cluster
# Requester -- request a distributed state machine operation

class Node(object):
    unique_ids = itertools.count()

    def __init__(self, network, address):
        self.network = network
        self.address = address or 'N%d' % next(self.unique_ids)
        self.logger = SimTimeLogger(logging.getLogger(self.address), {'network': self.network})
        self.logger.info('starting')
        self.roles = []
        self.send = functools.partial(self.network.send, self)

    def register(self, role):
        self.roles.append(role)

    def unregister(self, role):
        self.roles.remove(role)

    def receive(self, sender, message):
        handler_name = 'do_%s' % type(message).__name__

        for comp in self.roles[:]:
            if not hasattr(comp, handler_name):
                continue
            comp.logger.debug('received %s from %s', message, sender)
            fn = getattr(comp, handler_name)
            fn(sender=sender, **message._asdict())


