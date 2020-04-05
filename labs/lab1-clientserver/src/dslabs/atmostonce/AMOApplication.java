package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    // Your code here...
    private final Map<Address, CurrentAMOResult> results = new HashMap<>();

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        // Your code here...
        Address sender = amoCommand.sender();
        if (!alreadyExecuted(amoCommand)) {
            int sequenceNum = amoCommand.sequenceNum();
            Result result = application.execute(amoCommand.command());
            AMOResult amoResult = new AMOResult(result, sender, sequenceNum);
            results.put(sender, new CurrentAMOResult(amoResult, sequenceNum));
        }
        return results.get(sender).result();
    }

    public Result executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }

        if (command instanceof AMOCommand) {
            return execute(command);
        }

        return application.execute(command);
    }

    public boolean alreadyExecuted(AMOCommand amoCommand) {
        // Your code here...
        Address sender = amoCommand.sender();
        return results.containsKey(sender) &&
                results.get(sender).sequenceNum() >= amoCommand.sequenceNum();
    }

    @Data
    private static class CurrentAMOResult implements Serializable {
        private final AMOResult result;
        private final int sequenceNum;
    }
}
