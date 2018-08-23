from common import NULL_BALLOT, PREPARE_RETRANSMIT, ACCEPT_RETRANSMIT, LEADER_TIMEOUT, JOIN_RETRANSMIT, \
    INVOKE_RETRANSMIT
from message import Accepting, Promise, Accepted, Proposal, Propose, Invoked, Welcome, Prepare, Adopted, Preempted\
    , Accept, Decided, Ballot, Active, Join, Invoke, Decision
import itertools


class Role(object):
    def __init__(self, node):
        self.node = node
        self.node.register(self)
        self.running = True
        self.logger = node.logger.getChild(type(self).__name__)

    def set_timer(self, seconds, callback):
        return self.network.set_timer(self.node.address, seconds, lambda: self.running and callback())

    def stop(self):
        self.running = False
        self.node.unregister(self)


# Acceptor, ballot number is its most recent promise, set of accepted proposals for each slot
class Acceptor(Role):
    def __init__(self, node):
        super().__init__(node)
        self.ballot_num = NULL_BALLOT
        self.accepted_proposals = {}  # {slot: (ballot_num, proposal)}

    # come from scout, send back to scout
    def do_Prepare(self, sender, ballot_num):
        if ballot_num > self.ballot_num:
            self.ballot_num = ballot_num
            # we have heard a scout, so it might be the next leader
            self.node.send([self.node.address], Accepting(leader=sender))

        self.node.send([sender], Promise(
            ballot_num=self.ballot_num,
            accepted_proposals=self.accepted_proposals
        ))

    # come from commander, send back to commander
    def do_Accept(self, sender, ballot_num, slot, proposal):
        if ballot_num > self.ballot_num:
            self.ballot_num = ballot_num
            acc = self.accepted_proposals
            if slot not in acc or acc[slot][0] < ballot_num:
                acc[slot] = (ballot_num, proposal)

        self.node.send([sender], Accepted(
            slot=slot, ballot_num=ballot_num
        ))


# Replica 1.making new proposals
# 2.invoking the local state machine when proposals are decided
# 3.tracking the current leader
# 4.adding newly started nodes to the cluster
class Replica(Role):
    def __init__(self, node, execute_fn, state, slot, decisions, peers):
        super().__init__(node)
        self.execute_fn = execute_fn
        self.state = state
        self.slot = slot
        self.decisions = decisions  # {slot: proposal}
        self.peers = peers
        self.proposals = {}  # {slot: proposal}
        # next slot num for a proposal (may lead slot)
        self.next_slot = slot
        self.latest_leader = None
        self.latest_leader_timeout = None

    # come from client, send to leader, caller is client's address
    def do_Invoke(self, sender, caller, client_id, input_value):
        proposal = Proposal(caller, client_id, input_value)
        slot = next((s for s, p in self.proposals.items() if p == proposal), None)
        # propose, or re-propose if this proposal already has a slot
        self.propose(proposal, slot)

    def propose(self, proposal, slot=None):
        """send (or resend if slot is specified) a proposal"""
        if not slot:
            slot, self.next_slot = self.next_slot, self.next_slot + 1
        self.proposals[slot] = proposal
        #  find a leader we think is working - either the latest we know of, or
        #  ourselves (which may trigger a scout to make us the leader)
        leader = self.latest_leader or self.node.address
        self.logger.info('proposing %s at slot %d to leader %s' % (proposal, slot, leader))
        self.node.send([leader], Propose(slot=slot, proposal=proposal))

    #  come from leader, send to client
    def do_Decision(self, sender, slot, proposal):
        assert not self.decisions.get(self.slot, None), 'next slot to commit is already decided'
        if slot in self.decisions:
            assert self.decisions[slot] == proposal, 'slot %d already decided with %r!' % (slot, self.decisions(slot))
            return
        self.decisions[slot] = proposal
        self.next_slot = max(self.next_slot, slot + 1)

        #  re-propose our proposal in a new slot if it lost its slot and wasn't a no-op
        our_proposal = self.proposals.get(slot)
        if our_proposal is not None and our_proposal != proposal and our_proposal.caller:
            self.propose(our_proposal)

        #  execute any pending, decided proposals
        while True:
            commit_proposal = self.decisions.get(self.slot)
            if not commit_proposal:
                break   # not decided yet
            commit_slot, self.slot = self.slot, self.slot + 1
            self.commit(commit_slot, commit_proposal)

    def commit(self, slot, proposal):
        """Actually commit a proposal that is decided and in sequence"""
        decided_proposals = [p for s, p in self.decisions.items() if s < slot]
        if proposal in decided_proposals:
            self.logger.info('not committing duplicate proposal %r, slot %d', proposal, slot)
            return  # duplicate

        self.logger.info('committing %r at slot %d' % (proposal, slot))
        if proposal.caller is not None:
            #  perform a client operation
            self.state, output = self.execute_fn(self.state, proposal.input)
            self.node.send([proposal.caller], Invoked(client_id=proposal.client_id, output=output))

    #  tracking the leader

    def do_Adopted(self, sender, ballot_num, accepted_proposals):
        self.latest_leader = self.node.address
        self.leader_alive()

    def do_Accepting(self, sender, leader):
        self.latest_leader = leader
        self.leader_alive()

    def do_Active(self, sender):
        if sender != self.latest_leader:
            return
        self.leader_alive()

    def leader_alive(self):
        if self.latest_leader_timeout:
            self.latest_leader_timeout.cancel()

        def reset_leader():
            idx = self.peers.index(self.latest_leader)
            self.latest_leader = self.peers[(idx + 1) % len(self.peers)]
            self.logger.debug("leader time out; trying the next one, %s", self.latest_leader)
        self.latest_leader_timeout = self.set_timer(LEADER_TIMEOUT, reset_leader())

    #  adding new cluster members
    def do_Join(self, sender):
        if sender in self.peers:
            self.node.send([sender], Welcome(state=self.state, slot=self.slot, decisions=self.decisions))


# the leader create a scout role when it wants to become active, in response to receiving a Propose when it is inactive
# the scout sends(and re-sends if necessary) a Prepare,
# and collects Promise responses until hear majority or until it has been preempted
class Scout(Role):
    def __init__(self, node, ballot_num, peers):
        super().__init__(node)
        self.ballet_num = ballot_num
        self.accepted_proposals = {}  # {slot: (ballot_num, proposal)}
        self.acceptors = set()
        self.peers = peers
        self.quorum = len(self.peers) // 2 + 1
        self.retransmit_timer = None

    def start(self):
        self.logger.info("scout starting")
        self.send_prepare()

    def send_prepare(self):
        self.node.send(self.peers, Prepare(ballot_num=self.ballot_num))
        self.retransmit_timer = self.set_timer(PREPARE_RETRANSMIT, self.send_prepare)

    def update_accepted(self, accepted_proposals):
        acc = self.accepted_proposals
        for slot, (ballot_num, proposal) in accepted_proposals.items():
            if slot not in acc or acc[slot][0] < ballot_num:
                acc[slot] = (ballot_num, proposal)

    def do_Promise(self, sender, ballot_num, accepted_proposals):
        if ballot_num == self.ballet_num:
            self.logger.info('got matching promise; need %d' % self.quorum)
            self.update_accepted(accepted_proposals)
            self.acceptors.add(sender)
            if len(self.acceptors) >= self.quorum:
                accepted_proposals = {s: p for s, (_, p) in self.accepted_proposals.items()}
                self.node.send([self.node.address], Adopted(ballot_num=ballot_num,
                                                            accepted_proposals=accepted_proposals))
                self.stop()
        else:
            #  this acceptor has promised another leader a higher ballot number
            #  so we lost
            self.node.send([self.node.address], Preempted(slot=None, preempted_by=ballot_num))
            self.stop()


# leader create a commander role for each slot where it has an active proposal
# the commander sends(and re-sends if necessary)
# if majority of acceptors accepted, send decided to leader, send decision to all other nodes
# else if preempted, send preempted to leader
class Commander(Role):
    def __init__(self, node, ballot_num, slot, proposal, peers):
        super().__init__(node)
        self.ballot_num = ballot_num
        self.slot = slot
        self.proposal = proposal
        self.acceptors = set()
        self.peers = peers
        self.quorum = len(peers) // 2 + 1

    def start(self):
        self.node.send(set(self.peers) - self.acceptors, Accept(
            slot=self.slot, ballot_num=self.ballot_num, proposal=self.proposal
        ))
        self.set_timer(ACCEPT_RETRANSMIT, self.start)

    def finished(self, ballot_num, preempted):
        if preempted:
            self.node.send([self.node.address], Preempted(
                slot=self.slot, preempted_by=ballot_num
            ))
        else:
            self.node.send([self.node.address], Decided(
                slot=self.slot
            ))
        self.stop()

    def do_Accepted(self, sender, slot, ballot_num):
        if slot != self.slot:
            return
        if ballot_num == self.ballot_num:
            self.acceptors.add(sender)
            if len(self.acceptors) < self.quorum:
                return
            self.node.send(self.peers, Decision(
                slot=self.slot, proposal=self.proposal
            ))
            self.finished(ballot_num, preempted=False)
        else:
            self.finished(ballot_num, preempted=True)


class Leader(Role):
    def __init__(self, node, peers, commander_cls=Commander, scout_cls=Scout):
        super().__init__(node)
        self.ballot_num = Ballot(n=0, leader=node.address)
        self.active = False
        self.proposals = {}  # {slot: proposal}
        self.commander_cls = commander_cls
        self.scout_cls = scout_cls
        self.scouting = False
        self.peers = peers

    def start(self):
        # remind others we are active before LEADER_TIMEOUT expires
        def active():
            if self.active:
                self.node.send(self.peers, Active())
            self.set_timer(LEADER_TIMEOUT / 2.0, active)
        active()

    def spawn_scout(self):
        assert not self.scouting
        self.scouting = True
        self.scout_cls(self.node, self.ballot_num, self.peers).start()

    def do_Adopted(self, sender, ballot_num, accepted_proposals):
        self.scouting = False
        self.proposals.update(accepted_proposals)
        # do not re-spawn commanders here; if there are undecided proposals, the replicas will re-spawn
        self.logger.info('leader becoming active')
        self.active = True

    def spawn_commander(self, ballot_num, slot):
        proposal = self.proposals[slot]
        self.commander_cls(self.node, ballot_num=ballot_num, slot=slot, proposal=proposal, peers=self.peers).start()

    def do_Preempted(self, sender, slot, preempted_by):
        if not slot:  # from the scout
            self.scouting = False
        self.logger.info('leader preempted by %s', preempted_by.leader)
        self.active = False
        self.ballot_num = Ballot((preempted_by or self.ballot_num).n + 1, self.ballot_num.leader)

    def do_Propose(self, sender, slot, proposal):
        if slot not in self.proposals:
            if self.active:
                self.proposals[slot] = proposal
                self.logger.info('spawning commander for slot %d' % slot)
                self.spawn_commander(self.ballot_num, slot)
            else:
                if not self.scouting:
                    self.logger.info('got Propose when not active')
                else:
                    self.logger.info('got Propose when scouting')
        else:
            self.logger.info('got Propose for a slot already being Proposed')


# when a node joins cluster, Bootstrap sends Join to Replicas, Replicas reply Welcome
class Bootstrap(Role):
    def __init__(self, node, peers, execute_fn,
                 replica_cls=Replica,
                 acceptor_cls=Replica,
                 leader_cls=Leader,
                 commander_cls=Commander,
                 scout_cls=Scout):
        super().__init__(node)
        self.execute_fn = execute_fn
        self.peers = peers
        self.peers_cycle = itertools.cycle(peers)
        self.replica_cls = replica_cls
        self.acceptor_cls = acceptor_cls
        self.leader_cls = leader_cls
        self.commander_cls = commander_cls
        self.scout_cls = scout_cls

    def start(self):
        self.join()

    def join(self):
        self.node.send([next(self.peers_cycle)], Join())
        self.set_timer(JOIN_RETRANSMIT, self.join)

    def do_Welcome(self, sender, state, slot, decisions):
        self.acceptor_cls(self.node)
        self.replica_cls(self.node, execute_fn=self.execute_fn, peers=self.peers,
                         state=state, slot=slot, decisions=decisions)
        self.leader_cls(self.node, peers=self.peers, commander_cls=self.commander_cls,
                        scout_cls=self.scout_cls).start()
        self.stop()


class Seed(Role):
    def __init__(self, node, initial_state, execute_fn, peers, bootstrap_cls=Bootstrap):
        super().__init__(node)
        self.initial_state = initial_state
        self.execute_fn = execute_fn
        self.peers = peers
        self.bootstrap_cls = bootstrap_cls
        self.seen_peers = set()
        self.exit_timer = None

    def do_Join(self, sender):
        self.seen_peers.add(sender)
        if len(self.seen_peers) <= len(self.peers) // 2:
            return

        # cluster is ready, welcome everyone
        self.node.send(list(self.seen_peers), Welcome(
            state=self.initial_state, slot=1, decisions={}
        ))

        # stick around for long enough that we don't hear any new Join from the newly formatted cluster
        if self.exit_timer:
            self.exit_timer.cancel()
        self.exit_timer = self.set_timer(JOIN_RETRANSMIT * 2, self.finish)

    def finish(self):
        # add self to the cluster
        bs = self.bootstrap_cls(node=self.node,
                                peers=self.peers,
                                execute_fn=self.execute_fn)
        bs.start()
        self.stop()


class Requester(Role):
    client_ids = itertools.count(start=100000)

    def __init__(self, node, n, callback):
        super().__init__(node)
        self.client_id = next(self.client_ids)
        self.n = n
        self.output = None
        self.callback = callback
        self.invoke_timer = None

    def start(self):
        self.node.send([self.node.address], Invoke(
            caller=self.node.address, client_id=self.client_id, input_value=self.n
        ))
        self.invoke_timer = self.set_timer(INVOKE_RETRANSMIT, self.start)

    def do_Invoked(self, sender, client_id, output):
        if client_id != self.client_id:
            return
        self.logger.debug("received output %r" % output)
        self.invoke_timer.cancel()
        self.callback(output)
        self.stop()


# The replica creates new proposals in response to Invoke messages from clients,
# selecting what it believes to be an unused slot and sending a Propose message to the current leader.
# Furthermore, if the consensus for the selected slot is for a different proposal,
# the replica must re-propose with a new slot.
#
#                             Local
# Requester    ---------     Replica                  Current
#     *--->>---/ Invoke /--------+                      Leader
#              ---------         :         ----------
#                                *--->>---/ Propose /-----+
#                                         ----------      :
#                                                   (multi-paxos)
#                                         -----------     :
#                                +-------/ Decision /-<<--*
#               ----------       :       -----------
#     *--------/ Invoked /---<<--*
#     :        ----------

# When the leader role becomes active, it sends an Adopted message to the replica on the same node.
# The replica creates new proposals in response to Invoke messages from clients,
# selecting what it believes to be an unused slot and sending a Propose message to the current leader.
# Furthermore, if the consensus for the selected slot is for a different proposal,
# the replica must re-propose with a new slot.
#
#                             Local
# Leader      ----------     Replica
#   *--->>---/ Adopted /--------+
#            ----------

# When the acceptor role sends a Promise to a new leader, it sends an Accepting message to its local replica.
#
#                               Local
# Acceptor     ------------    Replica
#     *--->>--/ Accepting /-------+
#             ------------

# The active leader sends Active messages as a heartbeat.
# If no such message arrives before the LEADER_TIMEOUT expires,
# the replica assumes the leader is dead and moves on to the next leader.
# In this case, it's important that all replicas choose the same new leader,
# which we accomplish by sorting the members and selecting the next one in the list.
#
# Leader      ---------    Replica   Replica   Replica
#    *--->>--/ Active /-------+---------+---------+
#            ---------

# Finally, when a node joins the network, the bootstrap role sends a Join message.
# The replica responds with a Welcome message containing its most recent state,
# allowing the new node to come up to speed quickly.
#
# Bootstrap   -------    Replica   Replica   Replica
#     *-->>--/ Join /-------+---------+---------+
#     :      -------        :         :         :
#     :     ----------      :         :         :
#     +----/ Welcome /--<<--*         :         :
#          ----------                 :         :
#                ----------           :         :
#     +---------/ Welcome /----<<-----*         :
#               ----------                      :
#                     ----------                :
#     +--------------/ Welcome /------<<--------*
#                    ----------

# The leader creates a scout role when it wants to become active,
# in response to receiving a Propose when it is inactive.
# The scout sends (and re-sends, if necessary) a Prepare message,
# and collects Promise responses until it has heard from a majority of its peers or until it has been preempted.
# It communicates the result back to the leader with an Adopted or Preempted message, respectively.
#
#                          Scout        ----------     Acceptor        Acceptor        Acceptor
#                            *--->>----/ Prepare /--------+---------------+---------------+
#                            :         ----------         :               :               :
#                            :       ----------           :               :               :
#                            +------/ Promise /----<<-----*               :               :
#                            :      ----------                            :               :
#                            :                  ----------                :               :
#                            +-----------------/ Promise /-------<<-------*               :
#                            :                 ----------                                 :
#                            :                              ----------                    :
#                            +-----------------------------/ Promise /---------<<---------*
# Leader    ----------       :                             ----------
#   +------/ Adopted /---<<--*
#          ----------















