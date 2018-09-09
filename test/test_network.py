import mock
import unittest
from role import Role
from network import Network
from message import Join


class TestComp(Role):
    join_called = False

    def do_Join(self, sender):
        self.join_called = True
        # self.kill()


class NetworkTests(unittest.TestCase):

    def setUp(self):
        self.network = Network(1234)

    def kill(self, node):
        del self.network.nodes[node.address]

    def test_comm(self):
        """Node can successfully send a message between instances"""
        sender = self.network.new_node('S')
        receiver = self.network.new_node('R')
        comp = TestComp(receiver)
        comp.kill = lambda: self.kill(receiver)
        sender.send([receiver.address], Join())
        self.network.run()
        self.assertTrue(comp.join_called)

    def test_timeout(self):
        pass

    def test_cancel_timeout(self):
        pass
