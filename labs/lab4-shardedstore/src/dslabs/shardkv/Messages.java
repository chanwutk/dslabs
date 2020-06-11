package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.kvstore.KVStore;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.Map;
import java.util.Set;
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
    private final int configNum;
}

// Your code here...
@Data
final class ShardMoveMessage implements Message {
    @NonNull private final ShardMove command;
}

@Data
final class ShardMove implements Command {
    @NonNull private final Map<Integer, AMOApplication<KVStore>> apps;
    private final int configNum;
    @NonNull private Address sender;
}

@Data
final class ShardMoveAckMessage implements Message {
    @NonNull private final ShardMoveAck command;
}

@Data
final class ShardMoveAck implements Command {
    @NonNull private final Set<Integer> shards;
    private final int configNum;
}

@Data
final class NewConfig implements Command {
    private final ShardConfig config;
}
