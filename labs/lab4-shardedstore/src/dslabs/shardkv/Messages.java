package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;
import lombok.NonNull;

@Data
final class ShardStoreRequest implements Message {
    // Your code here...
    @NonNull private final AMOCommand amoCommand;
}

@Data
final class ShardStoreReply implements Message {
    // Your code here...
    private final AMOResult amoResult;
}

// Your code here...
