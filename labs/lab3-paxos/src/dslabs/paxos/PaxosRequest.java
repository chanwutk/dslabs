package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import lombok.Data;
import lombok.NonNull;

@Data
public final class PaxosRequest implements Message {
    // Your code here...
    @NonNull private final AMOCommand amoCommand;
}
