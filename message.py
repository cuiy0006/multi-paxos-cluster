from collections import namedtuple


# data type
Proposal = namedtuple('Proposal', ['caller', 'client_id', 'input'])
Ballot = namedtuple('Ballot', ['n', 'leader'])

# message type

# The leader creates a scout role when it wants to be alive
# scout -> Acceptor : Prepare
# acceptor -> scout : Promise
# acceptor -> local replica : Accepting ----- when acceptor sends Promise to new leader
Prepare = namedtuple('Prepare', ['ballot_num'])
Promise = namedtuple('Promise', ['ballot_num', 'accepted_proposals'])
Accepting = namedtuple('Accepting', ['leader'])
# **Discard this?** leader -> local replica : Adopted ----- when leader becomes alive
# scout -> leader : Adopted ----- succeed in election
# scout -> leader : Preempted ------ failed in election
Adopted = namedtuple('Adopted', ['ballot_num', 'accepted_proposals'])
Preempted = namedtuple('Preempted', ['slot', 'preempted_by'])

# The leader creates a commander role for each slot where it has an active proposal
# commander -> acceptor : Accept
# acceptor -> commander : Accepted
# Commander -> Leader : Decided
# Commander -> Leader : Preempted
Accept = namedtuple('Accept', ['slot', 'ballot_num', 'proposal'])
Accepted = namedtuple('Accepted', ['slot', 'ballot_num'])
Decided = namedtuple('Decided', ['slot'])

# client -> local replica : Invoke
# local replica -> leader : Propose
# leader -> local replica : Decision
# local replica -> client : Invoked
Invoke = namedtuple('Invoke', ['caller', 'client_id', 'input_value'])
Propose = namedtuple('Propose', ['slot', 'proposal'])
Decision = namedtuple('Decision', ['slot', 'proposal'])
Invoked = namedtuple('Invoked', ['client_id', 'output'])

# leader -> replica : Active ----- heartbeat
Active = namedtuple('Active', [])

# bootstrap -> replica : Join
# replica -> bootstrap : Welcome
Join = namedtuple('Join', [])
Welcome = namedtuple('Welcome', ['state', 'slot', 'decisions'])






