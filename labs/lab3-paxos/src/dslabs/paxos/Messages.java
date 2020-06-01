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
    private final int slot;
    @NonNull private final AMOCommand amoCommand;
}

@Data
class DecisionMessage implements Message {
    @NonNull private final LogEntry entry;
    private final int slot;
}

@Data
class P1aMessage implements Message {
    @NonNull private final BallotNum ballot_num;
}

@Data
class P1bMessage implements Message {
    @NonNull private final BallotNum ballot_num;
    @NonNull private final BallotNum p1a_ballot;
    @NonNull private final Map<Integer, LogEntry> log;
    private final boolean accepted;
}

@Data
class P2aMessage implements Message {
    @NonNull private final BallotNum ballot_num;

    // if null -> no-op
    private final AMOCommand amoCommand;
    private final int slot;
    private final int seq;
}

@Data
class P2bMessage implements Message {
    @NonNull private final LogEntry entry;
    private final int slot;
    private final boolean accepted;
    private final int seq;
}

@Data
class Heartbeat implements Message {
    @NonNull private final BallotNum leader_id;
    @NonNull private final Map<Integer, LogEntry> log;
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
class LogEntry implements Comparable<LogEntry>, Serializable {
    // amoCommand can be null if no-op
    private final AMOCommand amoCommand;
    @NonNull private final PaxosLogSlotStatus status;
    @NonNull private final BallotNum ballot_num;

    @Override
    public int compareTo(LogEntry o) {
        if (o == null) {
            return -1;
        }
        return ballot_num.compareTo(o.ballot_num);
    }

    public LogEntry choose() {
        return new LogEntry(amoCommand, CHOSEN, ballot_num);
    }

    public LogEntry increment(BallotNum ballot_num_) {
        return new LogEntry(amoCommand, status, ballot_num_);
    }
}

@Data
class NewLeader implements Message {
    private final Address leader;
    @NonNull private final BallotNum leader_id;
}