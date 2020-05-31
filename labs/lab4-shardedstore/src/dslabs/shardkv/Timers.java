package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Timer;
import lombok.Data;
import lombok.NonNull;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    @NonNull private final ShardStoreRequest shardStoreRequest;
}

// Your code here...
@Data
final class QueryTimer implements Timer {
    static final int QUERY_MILLIS = 50;
}
