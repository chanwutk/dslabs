package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Timer;
import java.util.Set;
import lombok.Data;
import lombok.NonNull;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    @NonNull private final ShardStoreRequest shardStoreRequest;
    private final int time;

    public ClientTimer nextTimer() {
        return new ClientTimer(shardStoreRequest, time + 1);
    }
}

// Your code here...
@Data
final class QueryTimer implements Timer {
    static final int QUERY_MILLIS = 50;

    private final int configNum;
}

@Data
final class ShardMoveTimer implements Timer {
    static final int SHARD_MOVE_MILLIS = 50;

    @NonNull private final ShardMoveMessage message;
    @NonNull private final Set<Address> dests;
}
