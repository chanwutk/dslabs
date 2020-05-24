package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
    public static final int INITIAL_CONFIG_NUM = 0;

    private final int numShards;

    // Your code here...
    private List<ShardConfig> shardConfigList;

    public ShardMaster(int numShards) {
        this.numShards = numShards;
        this.shardConfigList = new ArrayList<>();
    }

    public interface ShardMasterCommand extends Command {
    }

    @Data
    public static final class Join implements ShardMasterCommand {
        private final int groupId;
        private final Set<Address> servers;
    }

    @Data
    public static final class Leave implements ShardMasterCommand {
        private final int groupId;
    }

    @Data
    public static final class Move implements ShardMasterCommand {
        private final int groupId;
        private final int shardNum;
    }

    @Data
    public static final class Query implements ShardMasterCommand {
        private final int configNum;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    public interface ShardMasterResult extends Result {
    }

    @Data
    public static final class Ok implements ShardMasterResult {
    }

    @Data
    public static final class Error implements ShardMasterResult {
    }

    @Data
    public static final class ShardConfig implements ShardMasterResult {
        private final int configNum;

        // groupId -> <group members, shard numbers>
        private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;

    }


    @Override
    public Result execute(Command command) {
        if (command instanceof Join) {
            Join join = (Join) command;

            // Your code here...
            int groupId = join.groupId();
            Set<Address> servers = join.servers();
            int lastConfigNum = shardConfigList.size() - 1;
            Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo =
                    new HashMap<>();
            if (lastConfigNum < INITIAL_CONFIG_NUM) {
                Set<Integer> shardIds = new HashSet<>();
                for (int i = 0; i < numShards; i++) {shardIds.add(i);}
                newGroupInfo.put(INITIAL_CONFIG_NUM,new ImmutablePair<>(servers, shardIds));
                return new ShardConfig(INITIAL_CONFIG_NUM, newGroupInfo);
            }
            else {
                Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                        shardConfigList.get(lastConfigNum).groupInfo();
                if (groupInfo.containsKey(groupId)) {
                    return new Error();
                }
                // TODO: redistribute the shards
                return new ShardConfig(lastConfigNum + 1, newGroupInfo);
            }
        }

        if (command instanceof Leave) {
            Leave leave = (Leave) command;

            // Your code here...
        }

        if (command instanceof Move) {
            Move move = (Move) command;

            // Your code here...
        }

        if (command instanceof Query) {
            Query query = (Query) command;

            // Your code here...
            int configNum = query.configNum();
            int lastConfigNum = shardConfigList.size() - 1;
            if (configNum >= 0 && configNum <= lastConfigNum) {
                return (Result) new ShardConfig(configNum,
                        shardConfigList.get(configNum).groupInfo());
            } else if (configNum == -1 || configNum > lastConfigNum) {
                return (Result) new ShardConfig(lastConfigNum,
                        shardConfigList.get(lastConfigNum).groupInfo());
            } else {
                return new Error();
            }
        }
        throw new IllegalArgumentException();
    }

    //util
}
