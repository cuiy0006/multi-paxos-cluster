import itertools
import functools
import logging
from common import SimTimeLogger


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


