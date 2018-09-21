from test import utils
import mock
from role import Bootstrap, Seed, Join, Welcome
from common import JOIN_RETRANSMIT


class Tests(utils.ComponentTestCase):

    def setUp(self):
        super().setUp()
        self.Bootstrap = mock.Mock(autospec=Bootstrap)
        self.execute_fn = mock.Mock()
        self.seed = Seed(
            self.node, initial_state='state',
            peers=['p1', 'p2', 'p3'],
            execute_fn=self.execute_fn,
            bootstrap_cls=self.Bootstrap
        )

    def test_JOIN(self):
        """
        Seed waits for quorum, then sends a WELCOME in response to every
        JOIN until 2* JOIN_RETRANSMIT seconds have passed with no JOINs
        """
        self.node.fake_message(Join(), sender='p1')
        self.assertNoMessages()
        self.node.fake_message(Join(), sender='p3')
        self.assertMessage(['p1', 'p3'], Welcome(
            state='state', slot=1, decisions={}
        ))

        self.network.tick(JOIN_RETRANSMIT)
        self.node.fake_message(Join(), sender='p2')
        self.assertMessage(['p1', 'p2', 'p3'], Welcome(
            state='state', slot=1, decisions={}
        ))

        self.network.tick(JOIN_RETRANSMIT * 2)
        self.assertNoMessages()
        self.assertUnregistered()
        self.Bootstrap.assert_called_with(node=self.node, peers=['p1', 'p2', 'p3'],
                                          execute_fn=self.execute_fn)
        self.Bootstrap().start.assert_called()



