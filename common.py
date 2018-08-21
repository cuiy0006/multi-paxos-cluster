import logging
from message import Ballot, Proposal


JOIN_RETRANSMIT = 0.7
CATCHUP_INTERVAL = 0.6
ACCEPT_RETRANSMIT = 1.0
PREPARE_RETRANSMIT = 1.0
INVOKE_RETRANSMIT = 0.5
LEADER_TIMEOUT = 1.0

NULL_BALLOT = Ballot(-1, -1)
NOOP_PROPOSAL = Proposal(None, None, None)


class SimTimeLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return 'T=%.3f %s' % (self.extra['network'].now, msg), kwargs

    def getChild(self, name):
        return self.__class__(self.logger.getChild(name),
                              {'network': self.extra['network']})



