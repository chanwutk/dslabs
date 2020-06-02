package dslabs.paxos;

import dslabs.framework.Command;
import dslabs.framework.Message;
import lombok.Data;

@Data
public class PaxosDecision implements Message {
    private final Command command;
    private final boolean toResponse;
}
