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
}
