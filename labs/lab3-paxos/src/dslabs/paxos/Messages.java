package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
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
    @NonNull private final int ballot_num;
}

@Data
class P1bMessage implements Message {
    @NonNull private final int ballot_num;
}

@Data
class P2aMessage implements Message {
    @NonNull private final int ballot_num;
}

@Data
class P2bMessage implements Message {
    @NonNull private final int ballot_num;
}