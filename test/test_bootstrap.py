import mock
from test import utils
from role import Replica, Acceptor, Leader, Commander, Scout, Bootstrap
from message import Join, Welcome
from common import JOIN_RETRANSMIT


class Tests(utils.ComponentTestCase):

    def setUp(self):
        super().setUp()
        self.cb_args = None
        self.execute_fn = mock.Mock()

        self.Replica = mock.Mock(autospec=Replica)
        self.Acceptor = mock.Mock(autospec=Acceptor)
        self.Leader = mock.Mock(autospec=Leader)
        self.Commander = mock.Mock(autospec=Commander)
        self.Scout = mock.Mock(autospec=Scout)

        self.bootstrap = Bootstrap(
            self.node,
            ['p1', 'p2', 'p3'],
            self.execute_fn,
            replica_cls=self.Replica,
            acceptor_cls=self.Acceptor,
            leader_cls=self.Leader,
            commander_cls=self.Commander,
            scout_cls=self.Scout
        )

    def test_retransmit(self):
        """After start(), the bootstrap sends JOIN to each node in sequence until hearing WELCOME"""
        self.bootstrap.start()
        for receiver_id in ('p1', 'p2', 'p3', 'p1'):
            self.assertMessage([receiver_id], Join())
            self.network.tick(JOIN_RETRANSMIT)
        self.assertMessage(['p2'], Join())

        # send welcome to bootstrap role in this.node
        self.node.fake_message(Welcome(state='state', slot='slot', decisions={}))
        self.Acceptor.assert_called_with(self.node)
        self.Replica.assert_called_with(self.node,
                                        execute_fn=self.execute_fn,
                                        decisions={},
                                        state='state',
                                        slot='slot',
                                        peers=['p1', 'p2', 'p3'])
        self.Leader.assert_called_with(self.node,
                                       peers=['p1', 'p2', 'p3'],
                                       commander_cls=self.Commander,
                                       scout_cls=self.Scout)
        self.Leader().start.assert_called_with()
        self.assertTimers([])
        self.assertUnregistered()
