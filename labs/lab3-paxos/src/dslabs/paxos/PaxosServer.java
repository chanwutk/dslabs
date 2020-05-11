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
    // Replica
    private List<AMOCommand> requests;
    private Set<> replica_proposals;
    private Set<> decisions;
    private int slot_in;
    private int slot_out;
    // Acceptor
    private Integer ballot_num;
    private Set<> accpeted; /**/
    // Leader
    private Set<> scout_accpetors;
    private Set<> commander_accpetors;
    private int slot_num;
    private boolean is_leader;
    private Map<> leader_proposals;

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
        // Replica
        this.slot_in = this.slot_out = 1;
        this.requests = new ArrayList<>();
        // Acceptor
        this.ballot_num = null;
        this.accepted = new Set<>();
        this.acceptors = new Set<>();
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
        return null;
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
        return null;
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
        return 1;
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
        return 0;
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
    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        //elect the leader

        //broadcast a P1aMessage
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
}
