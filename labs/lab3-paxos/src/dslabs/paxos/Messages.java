package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Message;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.NonNull;

import static dslabs.paxos.PaxosLogSlotStatus.CHOSEN;

// Your code here...
@Data
class ProposeMessage implements Message {
    @NonNull private final int slot;
    @NonNull private final AMOCommand amoCommand;
}

@Data
class DecisionMessage implements Message {
    @NonNull private final PaxosLogEntry entry;
    @NonNull private final int slot;
}

@Data
class P1aMessage implements Message {
    @NonNull private final BallotNum ballot_num;
}

@Data
class P1bMessage implements Message {
    @NonNull private final BallotNum ballot_num;
    @NonNull private final BallotNum p1a_ballot;
    @NonNull private final Map<Integer, PaxosLogEntry> log;
    private final boolean accepted;
}

@Data
class P2aMessage implements Message {
    @NonNull private final BallotNum ballot_num;

    // if null -> no-op
    private final AMOCommand amoCommand;
    private final int slot;
}

@Data
class P2bMessage implements Message {
    @NonNull private final PaxosLogEntry entry;
    private final int slot;
    private final boolean accepted;
}

@Data
class Heartbeat implements Message {
    @NonNull private final BallotNum ballot_num;
    @NonNull private final Map<Integer, PaxosLogEntry> log;
    private final int system_slot_in;
}

@Data
class HeartbeatResponse implements Message {
    private final int to_exec;
}

@Data
class BallotNum implements Comparable<BallotNum>, Serializable {
    private final int number;
    @NonNull private final Address address;

    @Override
    public int compareTo(BallotNum o) {
        return number == o.number ? address.compareTo(o.address) :
                number - o.number;
    }
}

@Data
class PaxosLogEntry implements Comparable<PaxosLogEntry>, Serializable {
    @NonNull private final AMOCommand amoCommand;
    @NonNull private final PaxosLogSlotStatus status;
    @NonNull private final BallotNum ballot_num;

    @Override
    public int compareTo(PaxosLogEntry o) {
        if (o == null) {
            return -1;
        }
        return ballot_num.compareTo(o.ballot_num);
    }

    public PaxosLogEntry choose() {
        return new PaxosLogEntry(amoCommand, CHOSEN, ballot_num);
    }

    public PaxosLogEntry increment(BallotNum ballot_num_) {
        return new PaxosLogEntry(amoCommand, status, ballot_num_);
    }
}
