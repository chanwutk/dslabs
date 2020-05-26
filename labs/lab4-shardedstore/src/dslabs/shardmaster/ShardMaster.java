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

    // history of shard config
    private final List<ShardConfig> shardConfigList;
    // assignment of each shard to group id (index -> shard id, entry -> group id)
//    private final List<Integer> currentGroupInfo;
    private final int[] currentGroupInfo;
    // group number of the next group
    private int nextGroupNumber;

    public ShardMaster(int numShards) {
        this.numShards = numShards;
        this.shardConfigList = new ArrayList<>();
//        this.currentGroupInfo = new ArrayList<>();
        this.currentGroupInfo = new int[numShards];
        this.nextGroupNumber = 0;
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
            return executeJoin(join);
        }

        if (command instanceof Leave) {
            Leave leave = (Leave) command;

            // Your code here...
            return executeLeave(leave);
        }

        if (command instanceof Move) {
            Move move = (Move) command;

            // Your code here...
            return executeMove(move);
        }

        if (command instanceof Query) {
            Query query = (Query) command;

            // Your code here...
            return executeQuery(query);
        }
        throw new IllegalArgumentException();
    }

    private Result executeJoin(Join join) {
        int groupId = join.groupId();
        Set<Address> servers = new HashSet<>(join.servers());
        int lastConfigNum = getLastConfigNum();
        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo =
                new HashMap<>();
        if (lastConfigNum < INITIAL_CONFIG_NUM) {
            // first config
            Set<Integer> shardIds = new HashSet<>();
            for (int i = 0; i < numShards; i++) {
                shardIds.add(i);
                currentGroupInfo[i] = groupId;
            }
            newGroupInfo.put(groupId, new ImmutablePair<>(servers, shardIds));
            ShardConfig newConfig = new ShardConfig(INITIAL_CONFIG_NUM, newGroupInfo);
            shardConfigList.add(newConfig);
            return newConfig;
        }

        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                shardConfigList.get(lastConfigNum).groupInfo();
        if (groupInfo.containsKey(groupId)) {
            // group id already exist
            return new Error();
        }

        newGroupInfo.putAll(groupInfo);

        List<Integer> curShardsSize = new ArrayList<>();
        int maxNum = 0;
        for (int gid: newGroupInfo.keySet()) {
            int shardSize = newGroupInfo.get(gid).getRight().size();
            curShardsSize.add(shardSize);
            if (shardSize > maxNum) {
                maxNum = shardSize;
            }
        }

//        int maxShardNum = maxNum - 1;
//        for (int addShard = 0; addShard < maxShardNum; maxShardNum--) {
//            for (int shardSize: curShardsSize) {
//                if (shardSize - maxShardNum > 0) {
//                    addShard++;
//                }
//            }
//        }
        int lo = 0, hi = maxNum;
        while (hi - lo >= 1) {
            int mid = lo + ((hi - lo) / 2);
            int excessShardsCount = getExcessShardsCount(mid, curShardsSize);
            if (excessShardsCount < mid) {
                hi = mid;
            } else {
                lo = mid;
                if (excessShardsCount == mid) {
                    break;
                }
            }
        }
        int maxShardNum = lo;

        Set<Integer> newJoinShard = new HashSet<>();
        for (int gid: newGroupInfo.keySet()) {
            Set<Integer> shardSet = newGroupInfo.get(gid).getRight();
            if (shardSet.size() > maxShardNum) {
                // distribute shards from this group to the joining group
                Set<Integer> newShards = new HashSet<>(shardSet);
                int removeShardNum = shardSet.size() - maxShardNum;
                int i = 0;
                for (int shard: shardSet) {
                    if (i >= removeShardNum || newJoinShard.size() >= maxShardNum) {
                        break;
                    }
                    newShards.remove(shard);
                    newJoinShard.add(shard);
                    currentGroupInfo[shard] = groupId;
                    i++;
                }

                newGroupInfo.put(gid, new ImmutablePair<>(
                        newGroupInfo.get(gid).getLeft(), newShards));
            }

            if (newJoinShard.size() >= maxShardNum) {
                // joining group is full
                break;
            }
        }
        newGroupInfo.put(groupId, new ImmutablePair<>(servers, newJoinShard));
        ShardConfig newConfig = new ShardConfig(lastConfigNum + 1, newGroupInfo);
        shardConfigList.add(newConfig);
        return new Ok();
    }

    private Result executeLeave(Leave leave) {
        int groupId = leave.groupId();
        int lastConfigNum = getLastConfigNum();
        if (lastConfigNum < INITIAL_CONFIG_NUM) {
            return new Error();
        } else {
            Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                    shardConfigList.get(lastConfigNum).groupInfo();
            if (!groupInfo.containsKey(groupId) || groupInfo.keySet().size() == 1) {
                return new Error();
            } else {
                Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo =
                        new HashMap<>();
                newGroupInfo.putAll(groupInfo);

                List<Integer> freeShards = new ArrayList<>(
                        newGroupInfo.get(groupId).getRight());
                int numFreeShards = freeShards.size();
                newGroupInfo.remove(groupId);

                List<Integer> curShardsSize = new ArrayList<>();
                for (int gid: newGroupInfo.keySet()) {
                    curShardsSize.add(newGroupInfo.get(gid).getRight().size());
                }

                int minShardNum = 1;
                for (; numFreeShards > 0; minShardNum++) {
                    for (int shardSize: curShardsSize) {
                        if (minShardNum > shardSize) {
                            numFreeShards--;
                        }
                    }
                }

                numFreeShards = freeShards.size();
                int curAssignShards = 0;
                for (int gid: newGroupInfo.keySet()) {
                    Set<Integer> shardSet = newGroupInfo.get(gid).getRight();
                    if (shardSet.size() < minShardNum) {
                        int addShardNum = minShardNum - shardSet.size();
                        Set<Integer> newShards = new HashSet<>(shardSet);
                        for (int i = 0; i < addShardNum && curAssignShards < numFreeShards; i++) {
                            int shardIndex = freeShards.get(curAssignShards);
                            newShards.add(shardIndex);
                            currentGroupInfo.set(shardIndex, gid);
                            curAssignShards++;
                        }
                        newGroupInfo.put(gid, new ImmutablePair<>(
                                newGroupInfo.get(gid).getLeft(), newShards));
                    }
                    if (curAssignShards >= numFreeShards) {
                        break;
                    }
                }
                ShardConfig newConfig = new ShardConfig(lastConfigNum + 1, newGroupInfo);
                shardConfigList.add(newConfig);
                return new Ok();
            }
        }
    }

    private Result executeMove(Move move) {
        int groupId = move.groupId();
        int shardNum = move.shardNum();
        int lastConfigNum = getLastConfigNum();
        if (lastConfigNum < INITIAL_CONFIG_NUM || shardNum < 0 ||
                shardNum >= numShards) {
            return new Error();
        } else {
            Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                    shardConfigList.get(lastConfigNum).groupInfo();
            if (!groupInfo.containsKey(groupId)) {
                return new Error();
            } else {
                Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo =
                        new HashMap<>();
                newGroupInfo.putAll(groupInfo);
                int oldGroupId = currentGroupInfo.get(shardNum);
                if (oldGroupId != groupId) {
                    currentGroupInfo.set(shardNum, groupId);

                    Pair<Set<Address>, Set<Integer>> fromGid = groupInfo.get(oldGroupId);
                    Pair<Set<Address>, Set<Integer>> toGid = groupInfo.get(groupId);

                    Set<Integer> newFromShard = new HashSet<>(fromGid.getRight());
                    Set<Integer> newToShard = new HashSet<>(toGid.getRight());

                    assert(newFromShard.contains(shardNum));
                    newFromShard.remove(shardNum);
                    newToShard.add(shardNum);

                    newGroupInfo.put(oldGroupId, new ImmutablePair<>(fromGid.getLeft(), newFromShard));
                    newGroupInfo.put(groupId, new ImmutablePair<>(toGid.getLeft(), newToShard));
                }
                ShardConfig newConfig = new ShardConfig(lastConfigNum + 1, newGroupInfo);
                shardConfigList.add(newConfig);
                return new Ok();
            }
        }
    }

    private Result executeQuery(Query query) {
        if (shardConfigList.isEmpty()) {
            // first config has not been created
            return new Error();
        }

        int configNum = query.configNum();
        int lastConfigNum = getLastConfigNum();
        if (configNum == -1 || configNum > lastConfigNum) {
            // get last config
            return new ShardConfig(lastConfigNum,
                    shardConfigList.get(lastConfigNum).groupInfo());
        }

        assert(configNum >= 0) : "config number should be >= -1";
        return new ShardConfig(configNum,
                shardConfigList.get(configNum).groupInfo());
    }

    //util
    private int getLastConfigNum() {
        return shardConfigList.size() - 1;
    }

    private int getExcessShardsCount(int shardsCountThreshold, List<Integer> shardsCounts) {
        int excessShardsCount = 0;
        for (int shardsCount: shardsCounts) {
            if (shardsCount > shardsCountThreshold) {
                excessShardsCount += shardsCount - shardsCountThreshold;
            }
        }
        return excessShardsCount;
    }
}
