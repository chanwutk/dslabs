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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    private final Address[] servers;

    // Your code here...
    private AMOApplication<Application> amoApplication;
    private Map<Integer, PaxosLog> paxos_log;
    private int slot_in;
    private int slot_out;
    private Address leader;
    private boolean is_leader_active;
    private HeartBeat current_hearbeat;
    private int processed;

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
    private int min_processed;
    private Set<Address> heartbeat_responsed;
    private BallotNum leader_ballot;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.amoApplication = new AMOApplication<>(app);

    }


    @Override
    public void init() {
        // Your code here...
        this.slot_in = this.slot_out = 1;
        this.paxos_log = new HashMap<Integer, PaxosLog>();
        this.leader = null;
        this.is_leader_active = false;

        // Replica
        this.requests = new ArrayList<>();
        // Acceptor
        this.ballot_num = null;
        this.accepted = new Set<>();
        this.acceptors = new Set<>();

        // TODO: leader -> move to the place where init leader
        this.servers_processed = new HashMap<>();

        set(new HeartBeatTimer(), HB_TIMER);
        set(new HeartBeatCheckTimer(), HB_CHECK_TIMER);
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
        if (m.ballot_num > this.ballot_num) {
            this.ballot_num = m.ballot_num;
        }
        send(new P1bMessage(this.ballot_num, accpeted), sender);
    }

    private void handleP2aMessage(P2aMessage m, Address sender) {
        if (m.ballot_num == this.ballot_num) {
            accpeted.add();
        }
        send(new P2bMessage(this.ballot_num, ), sender);
    }

    // Leader: Scout
    private void handleP1bMessage(P1bMessage m, Address sender) {
        if (this.ballot_num == m.ballot_num && is_waiting) {
            // remove m from waiting
            if (this.accpetors.size() > ...) {
                // Adopted message
                Map pmax = new HashMap<>();
                for (pv : m.accepted) {
                    if (!pmax.containsKey(pv.slot_num) || pmax.get(pv.slot_num) < pv.ballot_num) {
                        pmax.put(pv.slot_num, pv.ballot_num);
                        this.leader_proposals.put(pv.slot_num, pv.command);
                    }
                }

                for (sn : this.leader_proposals) {
                    // Commander
                    // Broadcast P2aMessage
                    // Add waitlist for this slot num
                }
            }
        } else {
            // Preempted message
            if (m.ballot_num > this.ballot_num) {
                this.ballot_num++;
                for (Address sv : servers) {
                    if (!Objects.equals(address, sv)) {
                        send(new P1aMessage(this.ballot_num), sv);
                    }
                }
            }
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
        if (!this.leader_proposals.containsKey(m.slot_num())) {
            this.leader_proposals.put(m.slot_num(), m.amoCommand());
            // Commander
            // Broadcast P2aMessage
            // Add waitlist for this slot num
        }
    }

    private void handleHeartbeatResponse(HeartbeatResponse m, Address sender) {
        if (Objects.equals(leader, address())) {
            min_processed = Math.min(min_processed, m.processed());
            heartbeat_responsed.add(sender);
        }
    }

    // Follower
    private void handleHeartbeat(Heartbeat m, Address sender) {
        if(current_heartbeat == null || m.ballot_num() >= current_hearbeat.ballot_num()) {
            current_hearbeat = m;
            leader = sender;
            is_leader_active = true;
            garbageCollect(m.min_processed());
            send(new HeartbeatResponse(processed), leader);
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onHeartbeatTimer(HeartbeatTimer t) {
        if (Objects.equals(address(), leader)) {
            if (heartbeat_responsed.size() == servers.size() - 1) {
                garbageCollect(min_processed);
                heartbeat_responsed.clear();
                min_processed = Integer.MAX_VALUE;
            }
            for (Address sv : servers) {
                if (!Objects.equals(address, sv)) {
                    send(new Heartbeat(this.leader_ballot, slot_in - 1), sv);
                }
            }
        }
        set(t, HB_TIMER);
    }

    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        //elect the leader

        // broadcast a P1aMessage
        Address address = address();
        boolean is_leader = Objects.equals(address, leader);
        assert(!is_leader || is_leader_active);
        if (!is_leader_active) {
            for (Address sv : servers) {
                if (!Objects.equals(address, sv)) {
                    // TODO: generate new ballot?
                    send(new P1aMessage(this.ballot_num), sv);
                }
            }
            // TODO: send P1a to self?
        } else if (!is_leader) {
            is_leader_active = false;
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
        assert(upto <= processed);
        assert(upto >= slot_in);
        for (; slot_in < upto; slot_in++) {
            paxos_log.remove(slot_in);
        }
    }


    @Data
    private static class PaxosLog {
        @NonNull private final AmoCommand amoCommand;
        private final AMOResult amoResult;
        @NonNull private final PaxosLogSlotStatus status;
        private final int id;
    }

    @Data
    private static class BallotNum implements Comparable<BallotNum> {
        private final int ballot_num;
        @NonNull private final Address address;

        @Override
        public int compareTo(BallotNum b1, BallotNum b2) {
            return b1.ballot_num == b2.ballot_num
                ? b1.address.compareTo(b2.address)
                : b1.ballot_num - b2.ballot_num;
        }
    }
}
