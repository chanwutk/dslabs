package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Message;
import lombok.Data;
import lombok.NonNull;

// Your code here...
@Data
class ProposeMessage implements Message {
    @NonNull private final int slot_num;
    @NonNull private final AMOCommand amoCommand;
}

@Data
class DecisionMessage implements Message {

}

@Data
class P1aMessage implements Message {
    @NonNull private final BallotNum ballot_num;
}

@Data
class P1bMessage implements Message {
    @NonNull private final BallotNum ballot_num;
    private final boolean accepted;
}

@Data
class P2aMessage implements Message {
    @NonNull private final BallotNum ballot_num;
}

@Data
class P2bMessage implements Message {
    @NonNull private final BallotNum ballot_num;
}

@Data
class Heartbeat implements Message {
    @NonNull private final BallotNum ballot_num;
    private final int min_executed;
}

@Data
class HeartbeatResponse implements Message {
    private final int executed;
}

@Data
class BallotNum implements Comparable<BallotNum> {
    private final int number;
    @NonNull private final Address address;

    @Override
    public int compareTo(BallotNum o) {
        return number == o.number ? address.compareTo(o.address) :
                number - o.number;
    }
}
