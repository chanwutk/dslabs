package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.HeartbeatCheckTimer.HB_CHECK_TIMER;
import static dslabs.paxos.HeartbeatTimer.HB_TIMER;
import static dslabs.paxos.P2aTimer.P2A_TIMER;
import static dslabs.paxos.PaxosLogSlotStatus.ACCEPTED;
import static dslabs.paxos.PaxosLogSlotStatus.CHOSEN;
import static dslabs.paxos.PaxosLogSlotStatus.CLEARED;
import static dslabs.paxos.PaxosLogSlotStatus.EMPTY;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    private final Address[] servers;

    // Your code here...
    // server's amo application
    private final AMOApplication<Application> amoApplication;
    // prevent calling address() over and over
    private final Address address;
    // log for proposal
    private Map<Integer, PaxosLogEntry> paxos_log;
    // number of servers to reach majority
    private final int majority;
    // index of first non-cleared slot
    private int slot_in;
    // index of last executed slot
    //    private int slot_executed;
    // index of next slot to execute
    private int slot_to_exec;
    // index + 1 of last entry in the log
    private int slot_out;
    // current leader known by this server
    private Address leader;
    // if the current leader is active in the current check timer cycle
    private boolean is_leader_alive;
    // previously received valid heartbeat
    private Heartbeat prev_heartbeat;
    // highest ballot number heard
    private BallotNum global_ballot;

    // Scouting Info
    // if this server is scouting
    private boolean is_scouting;
    // set of servers that accept this server's p1a
    private Set<Address> p1aAccepted;

    // Replica
    private List<AMOCommand> requests;

    // Acceptor
    private BallotNum ballot_num;
    //    private Set<> accpeted; /**/

    // Leader
    //    private Set<> scout_accpetors;
    //    private Set<> commander_accpetors;
    //    private int slot_num;
    //    private Map<> leader_proposals;
    // minimum index of the slot to be executed over the system
    private int min_slot_to_exec;
    // set of servers that have responded the heartbeat over the current period
    private Set<Address> heartbeat_responded;
    // ballot number when this leader was elected used as id
    private BallotNum leader_id;
    // set of servers that accept this leader's p2a
    private Map<Integer, Set<Address>> p2aAccepted;
    // if thie leader is voting
    private boolean is_voting;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.amoApplication = new AMOApplication<>(app);
        this.address = address;
        majority = (servers.length / 2) + 1;
    }


    @Override
    public void init() {
        // Your code here...
        this.slot_in = this.slot_to_exec = this.slot_out = 1;
        this.paxos_log = new HashMap<>();
        this.leader = null;
        this.is_leader_alive = false;
        this.prev_heartbeat =
                new Heartbeat(new BallotNum(-1, address), paxos_log, 0);
        this.is_scouting = false;
        this.p1aAccepted = new HashSet<>();
        this.ballot_num = this.global_ballot = new BallotNum(0, address);

        // Replica
        this.requests = new ArrayList<>();
        // Acceptor
        //        this.accepted = new Set<>();
        //        this.acceptors = new Set<>();

        // Leader
        this.heartbeat_responded = new HashSet<>();
        this.p2aAccepted = new HashMap<>();

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
            return CLEARED;
        } else if (!paxos_log.containsKey(logSlotNum)) {// else if (logSlotNum >= slot_out) {
            return EMPTY;
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
                if (paxos_log.get(logSlotNum).amoCommand() == null) {
                    // no-op
                    return null;
                }
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
        return slot_out - 1;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
        if (!isLeader()) {
            // only leader accepts request -> reject the message
            send(new PaxosReply(null), sender);
            return;
        }

        AMOCommand amoCommand = m.amoCommand();
        if (amoApplication.alreadyExecuted(amoCommand)) {
            // outdated request
            send(new PaxosReply(amoApplication.execute(amoCommand)), sender);
            return;
        }

        if (onGoingCommand(amoCommand)) {
            // still working on this command
            return;
        }

        // start p2a
        ballot_num = global_ballot = incrementBallot(global_ballot);
        PaxosLogEntry entry = new PaxosLogEntry(amoCommand, ACCEPTED, ballot_num);
        paxos_log.put(slot_out, entry);
        p2aAccepted.put(slot_out, new HashSet<>());
        slot_out++;
        //System.out.println("Add slot: " + slot_out + " " + amoCommand);
        onP2aTimer(
                new P2aTimer(new P2aMessage(ballot_num, amoCommand, slot_out - 1)));
    }

    // Your code here...
    // Replica
    private void handleDecisionMessage(DecisionMessage m, Address sender) {
        if (!Objects.equals(sender, leader)) {
            // only receives decision from leader
            return;
        }

        int slot = m.slot();
        if (status(slot) == CLEARED) {
            // outdated decision
            //System.out.println("Cleared: " + slot);
            return;
        }
        //System.out.println("Decision: " + slot);
        if (status(slot) != CHOSEN) {
            paxos_log.put(slot, m.entry());
        } else {
            PaxosLogEntry entry = paxos_log.get(slot);
            assert (Objects.equals(m.entry().amoCommand(), command(slot))) :
                    "conflicting chosen decisions (command)";
            assert (Objects
                    .equals(m.entry().ballot_num(), entry.ballot_num())) :
                    "conflicting chosen decision (ballot number)";
        }

        // execute every log from start
        sequentialExecute();
    }

    // Acceptor
    private void handleP1aMessage(P1aMessage m, Address sender) {
        if (leader != null) {
            // still have leader
            return;
        }

        boolean accepted = false;
        if (m.ballot_num().compareTo(global_ballot) >= 0) {
            // received higher ballot
            global_ballot = m.ballot_num();
            accepted = true;
        }

        P1bMessage p1b = new P1bMessage(global_ballot, paxos_log, accepted);
        if (Objects.equals(sender, address)) {
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
            global_ballot = m_ballot_num;
        }

        if (!Objects.equals(sender, leader)) {
            // only leader
            return;
        }

        int slot = m.slot();
        boolean accepted = false;
        PaxosLogEntry entry =
                new PaxosLogEntry(m.amoCommand(), ACCEPTED, m_ballot_num);
        switch (status(slot)) {
            case CHOSEN:
                // reject
                break;
            case EMPTY:
                // accept: new slot
                slot_out = Math.max(slot + 1, slot_out);
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
            default:
                // CLEARED: outdated message
                return;
        }

        P2bMessage p2b = new P2bMessage(paxos_log.get(slot), slot, accepted);
        if (Objects.equals(sender, address)) {
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

        // update its log from the message
        updateLog(m.log());

        int cmp = m.ballot_num().compareTo(ballot_num);
        if (cmp == 0 && m.accepted()) {
            // accepted
            p1aAccepted.add(sender);
            if (isMajority(p1aAccepted)) {
                // majority accepted -> becomes leader
                // clean up
                is_scouting = false;
                p1aAccepted.clear();
                leader = address;

                // leader init
                leader_id = ballot_num;
                prev_heartbeat =
                        new Heartbeat(leader_id, paxos_log, slot_in - 1);
                heartbeat_responded.clear();
                p2aAccepted.clear();

                // update ballot
                // TODO: should we update ballot??

                // propose all non-chosen slots
                proposeAll();

                // start heartbeat
                onHeartbeatTimer(new HeartbeatTimer(leader_id));
            }
        } else if (cmp > 0 && m.ballot_num().compareTo(global_ballot) > 0) {
            // received higher ballot number -> stop trying to be leader
            // and higher than global
            is_scouting = false;
            p1aAccepted.clear();
            leader = null;

            // update ballot_num
            global_ballot = m.ballot_num();
        }
    }

    // Leader: Commander
    private void handleP2bMessage(P2bMessage m, Address sender) {
        assert(!is_scouting) : "cannot vote while scouting";
        if (!isLeader()) {
            // only voting leader handles p2b
            return;
        }

        int slot = m.slot();
        PaxosLogEntry m_entry = m.entry();
        PaxosLogEntry entry = paxos_log.get(slot);
        int cmp = m_entry.compareTo(entry);
        if (cmp == 0 && m.accepted() && p2aAccepted.get(slot) != null ) {
            // accepted
            p2aAccepted.get(slot).add(sender);

            if (isMajority(p2aAccepted.get(slot))) {
                // majority accepted -> choose the entry
                // clean up
                //System.out.println("Majority: " + slot);
                is_voting = false;
                p2aAccepted.remove(slot);

                // choose the entry
                PaxosLogEntry chosenEntry = entry.choose();
                paxos_log.put(slot, chosenEntry);

                // broadcast decision
                DecisionMessage decision =
                        new DecisionMessage(chosenEntry, slot);
                for (Address server : servers) {
                    send(decision, server);
                }

                // execute chosen slots sequentially
                sequentialExecute();
            }
        } else if (cmp > 0 && status(slot) == ACCEPTED) {
            // preempted -> broadcast with higher ballot
            BallotNum m_ballot_num = m_entry.ballot_num();
            if (global_ballot.compareTo(m_ballot_num) < 0) {
                global_ballot = m_ballot_num;
            }
            global_ballot = ballot_num = incrementBallot(global_ballot);
            PaxosLogEntry newEntry = entry.increment(ballot_num);
            paxos_log.put(slot, newEntry);
            p2aAccepted.clear();

            P2aMessage p2a =
                    new P2aMessage(ballot_num, newEntry.amoCommand(), slot);
            onP2aTimer(new P2aTimer(p2a));
        }
    }

    // Leader
    // MICK: Since replicas drop all requests from client, so no proposal
    //    private void handleProposeMessage(ProposeMessage m, Address sender) {
    //        // TODO: implement this
    //        if (!this.leader_proposals.containsKey(m.slot())) {
    //            this.leader_proposals.put(m.slot(), m.amoCommand());
    //            // Commander
    //            // Broadcast P2aMessage
    //            // Add waitlist for this slot num
    //        }
    //    }

    private void handleHeartbeatResponse(HeartbeatResponse m, Address sender) {
        if (isLeader() && m.to_exec() >= slot_in) {
            // only leader receives heartbeat responses
            // and ignore outdated response
            min_slot_to_exec = Math.min(min_slot_to_exec, m.to_exec());
            heartbeat_responded.add(sender);
        }
    }

    // Follower
    private void handleHeartbeat(Heartbeat m, Address sender) {
        if (m.ballot_num().compareTo(prev_heartbeat.ballot_num()) < 0) {
            // heartbeat from old leader
            return;
        }
        if (m.system_slot_in() < slot_in) {
            // outdated heartbeat
            // TODO: if checking for outdated heartbeat, what else needs to be checked?
            return;
        }

        prev_heartbeat = m;
        leader = sender;
        is_leader_alive = true;
        is_scouting = false;
        updateLog(m.log());
        sequentialExecute();
        collectGarbage(m.system_slot_in() - 1);
        send(new HeartbeatResponse(slot_to_exec), leader);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onHeartbeatTimer(HeartbeatTimer t) {
        if (!isLeader() || !Objects.equals(t.leader_ballot(), leader_id)) {
            // no longer leader or outdated timer
            return;
        }

        if (heartbeat_responded.size() == servers.length - 1) {
            // heard back from all other servers
            collectGarbage(min_slot_to_exec - 1);
            heartbeat_responded.clear();
            min_slot_to_exec = slot_to_exec;
        }

        // broadcasts heartbeats
        for (Address sv : servers) {
            if (!Objects.equals(address, sv)) {
                send(new Heartbeat(this.leader_id, paxos_log, slot_in), sv);
            }
        }
        set(t, HB_TIMER);
    }

    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        assert (!isLeader() || is_leader_alive);
        if (!is_leader_alive) {
            // try to be leader
            // generate new ballot
            global_ballot = ballot_num = incrementBallot(global_ballot);

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

    private void onP2aTimer(P2aTimer t) {
        int slot = t.p2a().slot();
        if (slot < slot_in || paxos_log.get(slot).ballot_num().compareTo(t.p2a().ballot_num()) != 0) {
            // outdated timer
            return;
        }

        P2aMessage message = t.p2a();
        for (Address server : servers) {
            if (Objects.equals(server, address)) {
                handleP2aMessage(message, address);
            } else {
                send(message, server);
            }
        }

        set(t, P2A_TIMER);
    }


    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private AMOResult runAMOCommand(AMOCommand amoCommand) {
        return amoApplication.execute(amoCommand);
    }

    private void collectGarbage(int upto) {
        assert (upto < slot_to_exec);
        assert (slot_in - 1 <= upto);  // allow not garbage collecting
        for (; slot_in <= upto; slot_in++) {
            paxos_log.remove(slot_in);
        }
    }

    private boolean isLeader() {
        return Objects.equals(address, leader);
    }

    private BallotNum incrementBallot(BallotNum ballot_num) {
        return new BallotNum(ballot_num.number() + 1, address);
    }

    private <T> boolean isMajority(Collection<T> c) {
        return c.size() >= majority;
    }

    private void updateLog(Map<Integer, PaxosLogEntry> log) {
        for (int slot : log.keySet()) {
            if (slot < slot_in) {
                continue;
            }
            PaxosLogEntry entry = log.get(slot);
            if (slot + 1 > slot_out) {
                //System.out.println("Update slot out: " + slot_out);
                //System.out.println(paxos_log.keySet());
                slot_out = slot + 1;
                paxos_log.put(slot, entry);
            } else if (entry.status() == ACCEPTED) {
                // accepted
                switch (status(slot)) {
                    case ACCEPTED:
                        BallotNum ballot_num_ =
                                paxos_log.get(slot).ballot_num();
                        if (entry.ballot_num().compareTo(ballot_num_) >= 0) {
                            paxos_log.put(slot, entry);
                        }
                        break;
                    case EMPTY:
                        paxos_log.put(slot, entry);
                        break;
                }
            } else {
                // chosen
                paxos_log.put(slot, entry);
            }
        }
    }

    private void sequentialExecute() {
        for (; slot_to_exec < slot_out && status(slot_to_exec) == CHOSEN;
             slot_to_exec++) {
            AMOCommand amoCommand = paxos_log.get(slot_to_exec).amoCommand();
            if (amoCommand == null) {
                // no-op
                continue;
            }
            //System.out.println("Execute: " + slot_to_exec);
            AMOResult result =
                    runAMOCommand(paxos_log.get(slot_to_exec).amoCommand());
            send(new PaxosReply(result), amoCommand.sender());
        }
    }

    private void proposeAll() {
        // try to:
        //   choose: all the accepted slot
        //   no-op : all the empty slot
        for (int i = slot_in; i < slot_out; i++) {
            P2aMessage p2a = null;
            switch (status(i)) {
                case CLEARED:
                    assert (false) : "i > slot_in should not be cleared";
                case ACCEPTED:
                    AMOCommand c = paxos_log.get(i).amoCommand();
                    p2a = new P2aMessage(ballot_num, c, i);
                    break;
                case EMPTY:
                    p2a = new P2aMessage(ballot_num, null, i);
                    break;
            }
            if (p2a != null) {
                onP2aTimer(new P2aTimer(p2a));
            }
        }
    }

    private boolean onGoingCommand(AMOCommand amoCommand) {
        for (int slot : paxos_log.keySet()) {
            if (slot_to_exec <= slot && Objects.equals(amoCommand,
                    paxos_log.get(slot).amoCommand())) {
                return true;
            }
        }
        return false;
    }
}
