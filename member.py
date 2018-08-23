from role import Seed, Bootstrap
import threading
import queue


class Member(object):
    def __init__(self, state_machine, network, peers, seed=None, seed_cls=Seed, bootstrap_cls=Bootstrap):
        self.network = network
        self.node = network.new_node()
        if seed is not None:
            self.startup_role = seed_cls(self.node, initial_state=seed, peers=peers, execute_fn=state_machine)
        else:
            self.startup_role = bootstrap_cls(self.node, execute_fn=state_machine, peers=peers)
        self.requester = None
        self.thread = None

    def start(self):
        self.startup_role.start()
        self.thread = threading.Thread(target=self.network.run)
        self.thread.run()

    def invoke(self, input_value, request_cls=Requester):
        assert self.requester is None
        q = queue.Queue()
        self.requester = request_cls(self.node, input_value, q.put)
        self.requester.start()
        output = q.get()
        self.requester = None
        return output

