package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;
import lombok.NonNull;

@Data
public final class AMOCommand implements Command {
    // Your code here...
    @NonNull private final Command command;
    @NonNull private final Address sender;
    private final int sequenceNum;
    private final boolean executeReadOnly;

    public AMOCommand(Command command, Address sender, int sequenceNum, boolean executeReadOnly) {
        this.command = command;
        this.sender = sender;
        this.sequenceNum = sequenceNum;
        this.executeReadOnly = executeReadOnly;
    }

    public AMOCommand(Command command, Address sender, int sequenceNum) {
        this(command, sender, sequenceNum, false);
    }

    public AMOCommand(Command command, Address sender, boolean executeReadOnly) {
        this(command, sender, -1, executeReadOnly);
    }
}
