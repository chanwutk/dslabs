package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

import static dslabs.paxos.HeartbeatCheckTimer.HB_CHECK_TIMER;
import static dslabs.paxos.HeartbeatTimer.HB_TIMER;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    private final Address[] servers;

    // Your code here...
    // server's amo application
    private AMOApplication<Application> amoApplication;
    // log for proposal
    private Map<Integer, PaxosLogEntry> paxos_log;
    // number of servers to reach majority
    private final int majority;
    // index of first non-cleared slot
    private int slot_in;
    // index of last executed slot
    private int slot_executed;
    // index of last entry in the log
    private int slot_out;
    // current leader known by this server
    private Address leader;
    // if the current leader is active in the current check timer cycle
    private boolean is_leader_alive;
    // previously received valid heartbeat
    private Heartbeat prev_heartbeat;

    // Scouting Info
    // if this server is scouting
    private boolean is_scouting;
    // set of servers that accept this server's p1a
    private Set<Address> p1aAccepted;

    // Replica
    private List<AMOCommand> requests;
    private Set<> replica_proposals;
    private Set<> decisions;

    // Acceptor
    private BallotNum ballot_num;
    private Set<> accpeted; /**/

    // Leader
    private Set<> scout_accpetors;
    private Set<> commander_accpetors;
    private int slot_num;
    private Map<> leader_proposals;
    // minimum slot executed over the system
    private int min_slot_executed;
    // set of servers that have responded the heartbeat over the current period
    private Set<Address> heartbeat_responded;
    // ballot number when this leader was elected
    private BallotNum leader_ballot;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.amoApplication = new AMOApplication<>(app);
        majority = (servers.length / 2) + 1;
    }


    @Override
    public void init() {
        // Your code here...
        this.slot_in = 1;
        this.slot_executed = this.slot_out = 0;
        this.paxos_log = new HashMap<>();
        this.leader = null;
        this.is_leader_alive = false;
        this.prev_heartbeat = new Heartbeat(new BallotNum(-1, address()), 0);
        this.is_scouting = false;
        this.p1aAccepted = new HashSet<>();
        this.ballot_num = new BallotNum(0, address());

        // Replica
        this.requests = new ArrayList<>();
        // Acceptor
        this.accepted = new Set<>();
        this.acceptors = new Set<>();

        // Leader
        this.heartbeat_responded = new HashSet<>();
        // TODO: leader -> move to the place where init leader

        set(new HeartbeatCheckTimer(), HB_CHECK_TIMER);
    }

    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the servers's local log.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        // Your code here...
        if (logSlotNum < slot_in) {
            return PaxosLogSlotStatus.CLEARED;
        } else if (logSlotNum > slot_out) {
            return PaxosLogSlotStatus.EMPTY;
        }
        return paxos_log.get(logSlotNum).status();
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log. If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. If
     * clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     */
    public Command command(int logSlotNum) {
        // Your code here...
        PaxosLogSlotStatus log_status = status(logSlotNum);
        switch (log_status) {
            case CLEARED:
            case EMPTY:
                return null;
            default:
                return paxos_log.get(logSlotNum).amoCommand().command();
        }
    }

    /**
     * Return the index of the first non-cleared slot in the server's local
     * log.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int firstNonCleared() {
        // Your code here...
        return slot_in;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log. If
     * there are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int lastNonEmpty() {
        // Your code here...
        if (paxos_log.isEmpty()) {
            return 0;
        }
        return slot_out;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
        // queuing message
        // Replica
        this.requests.add(m.amoCommand());
    }

    // Your code here...
    // Replica
    private void handleDecisionMessage(DecisionMessage m, Address sender) {

    }

    // Acceptor
    private void handleP1aMessage(P1aMessage m, Address sender) {
        boolean accepted = false;
        if (m.ballot_num().compareTo(ballot_num) >= 0) {
            // received higher ballot
            this.ballot_num = m.ballot_num();
            accepted = true;
        }

        P1bMessage p1b = new P1bMessage(this.ballot_num, accepted);
        if (Objects.equals(sender, address())) {
            // response to self, bypassing network
            handleP1bMessage(p1b, sender);
        } else {
            // response to other through network
            send(p1b, sender);
        }
    }

    private void handleP2aMessage(P2aMessage m, Address sender) {
        BallotNum m_ballot_num = m.ballot_num();
        if (m_ballot_num.compareTo(ballot_num) >= 0) {
            // received higher ballot
            this.ballot_num = m_ballot_num;
        }

        int slot = m.slot();
        boolean accepted = false;
        PaxosLogEntry entry = new PaxosLogEntry(m.amoCommand(),
                PaxosLogSlotStatus.ACCEPTED, m_ballot_num);
        switch (status(slot)) {
            case CLEARED:
                // outdated message
                return;
            case CHOSEN:
                // reject
                break;
            case EMPTY:
                // accept: new slot
                slot_out = Math.max(slot, slot_out);
                paxos_log.put(slot, entry);
                accepted = true;
                break;
            case ACCEPTED:
                // accept?: slot has been proposed
                BallotNum slot_ballot_num = paxos_log.get(slot).ballot_num();
                if (m_ballot_num.compareTo(slot_ballot_num) >= 0) {
                    // replace accepted log if not older
                    paxos_log.put(slot, entry);
                    accepted = true;
                }
                break;
        }

        P2bMessage p2b = new P2bMessage(paxos_log.get(slot), slot, accepted);
        if (Objects.equals(sender, address())) {
            // response to self, bypassing network
            handleP2bMessage(p2b, sender);
        } else {
            // response to other through network
            send(p2b, sender);
        }
    }

    // Leader: Scout
    private void handleP1bMessage(P1bMessage m, Address sender) {
        if (!is_scouting) {
            return;
        }

        int cmp = m.ballot_num().compareTo(ballot_num);
        if (cmp == 0 && m.accepted()) {
            // accepted
            p1aAccepted.add(sender);
            if (isMajority(p1aAccepted.size())) {
                // majority accepted -> becomes leader
                is_scouting = false;
                p1aAccepted.clear();
                leader = address();

                // leader init
                leader_ballot = ballot_num;
                heartbeat_responded.clear();

                // start heartbeat
                onHeartbeatTimer(new HeartbeatTimer(ballot_num));
            }
        } else if (cmp > 0) {
            // received higher ballot number -> stop trying to be leader
            is_scouting = false;
            p1aAccepted.clear();
            leader = null;

            // update ballot_num
            ballot_num = m.ballot_num();
        }
    }

    // Leader: Commander
    private void handleP2bMessage(P2bMessage m, Address sender) {
        if (this.ballot_num == m.ballot_num && is_waiting) {
            // remove m from waiting
            if (this.accpetors.size() > ...) {
                // Decision message
                for (r : replicas) {
                    send(new DecisionMessage(), r);
                }
            }
        } else {
            // Preempted message
            this.ballot_num++;
            for (Address sv : servers) {
                if (!Objects.equals(address, sv)) {
                    send(new P1aMessage(this.ballot_num), sv);
                }
            }
        }
    }

    // Leader
    private void handleProposeMessage(ProposeMessage m, Address sender) {
        if (!this.leader_proposals.containsKey(m.slot())) {
            this.leader_proposals.put(m.slot(), m.amoCommand());
            // Commander
            // Broadcast P2aMessage
            // Add waitlist for this slot num
        }
    }

    private void handleHeartbeatResponse(HeartbeatResponse m, Address sender) {
        if (isLeader() && m.executed() >= slot_in - 1) {
            // only leader receives heartbeat responses
            // and ignore outdated response
            min_slot_executed = Math.min(min_slot_executed, m.executed());
            heartbeat_responded.add(sender);
        }
    }

    // Follower
    private void handleHeartbeat(Heartbeat m, Address sender) {
        if (m.ballot_num().compareTo(prev_heartbeat.ballot_num()) < 0) {
            // heartbeat from old leader
            return;
        }
        if (m.min_executed() < slot_in - 1) {
            // outdated heartbeat
            // TODO: if checking for outdated heartbeat, what else needs to be checked?
            return;
        }
        prev_heartbeat = m;
        leader = sender;
        is_leader_alive = true;
        garbageCollect(m.min_executed());
        send(new HeartbeatResponse(slot_executed), leader);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onHeartbeatTimer(HeartbeatTimer t) {
        if (!isLeader() || !Objects.equals(t.leader_ballot(), leader_ballot)) {
            // no longer leader or outdated timer
            return;
        }

        if (heartbeat_responded.size() == servers.length - 1) {
            // heard back from all other servers
            garbageCollect(min_slot_executed);
            heartbeat_responded.clear();
            min_slot_executed = Integer.MAX_VALUE;
        }

        // broadcasts heartbeats
        for (Address sv : servers) {
            if (!Objects.equals(address(), sv)) {
                send(new Heartbeat(this.leader_ballot, slot_in - 1), sv);
            }
        }
        set(t, HB_TIMER);
    }

    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        Address address = address();
        assert (!isLeader() || is_leader_alive);
        if (!is_leader_alive) {
            // try to be leader
            // generate new ballot
            // TODO: (after done with P1a) revise how to generate a new ballot
            ballot_num = incrementBallot(ballot_num);

            // scout P2aMessage
            p1aAccepted.clear();
            is_scouting = true;
            for (Address sv : servers) {
                if (!Objects.equals(address, sv)) {
                    send(new P1aMessage(ballot_num), sv);
                }
            }

            // scout P1aMessage to itself
            handleP1aMessage(new P1aMessage(ballot_num), address);
        } else if (!isLeader()) {
            // for follower with active leader
            // reset leader status for the next checking period
            is_leader_alive = false;
        }
        set(t, HB_CHECK_TIMER);
    }


    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    // Replica
    private void propose() {
        while (!this.requests.isEmpty()) {
            if (!this.decisions.contains(this.slot_in)) {
                AMOCommand cmd = this.requests.remove(0);
                this.replica_proposals.put(this.slot_in, cmd);
                send(new ProposeMessage(), leader);
            }
            this.slot_in++;
        }
    }

    private void perform(AMOCommand cmd) {
        for (int s = 1; s < this.slot_out; s++) {
            if (Objects.equals(this.decisions.get(s), cmd)) {
                AMOResult ret = runAMOCommand(cmd);
                send(ret, client);
            }
        }
        this.slot_out++;
    }

    private AMOResult runAMOCommand(AMOCommand amoCommand) {
        return amoApplication.execute(amoCommand);
    }

    private void garbageCollect(int upto) {
        assert (upto <= slot_executed);
        assert (slot_in - 1 <= upto);  // allow not garbage collecting
        for (; slot_in <= upto; slot_in++) {
            paxos_log.remove(slot_in);
        }
    }

    private boolean isLeader() {
        return Objects.equals(address(), leader);
    }

    private BallotNum incrementBallot(BallotNum ballot_num) {
        return new BallotNum(ballot_num.number() + 1, address());
    }

    private boolean isMajority(int num_servers) {
        return num_servers >= majority;
    }
}
