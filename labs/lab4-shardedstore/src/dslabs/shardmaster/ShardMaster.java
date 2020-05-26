package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
    private static final Comparator<Pair<Integer, Set<Integer>>> SIZE_CMP = Comparator.comparingInt(e -> e.getRight().size());
    private static final Comparator<Pair<Integer, Set<Integer>>> SIZE_GID_CMP = SIZE_CMP.thenComparingInt(Pair::getLeft);
    private static final Result OK = new Ok();
    private static final Result ERROR = new Error();

    // history of shard config
    private final List<ShardConfig> shardConfigList;
    // assignment of each shard
    private final int[] currentGroupInfo;

    public ShardMaster(int numShards) {
        this.numShards = numShards;
        this.shardConfigList = new ArrayList<>();
        this.currentGroupInfo = new int[numShards + 1];
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
            for (int i = 1; i <= numShards; i++) {
                shardIds.add(i);
                currentGroupInfo[i] = groupId;
            }
            newGroupInfo.put(groupId, p(servers, shardIds));
            ShardConfig newConfig = new ShardConfig(INITIAL_CONFIG_NUM, newGroupInfo);
            shardConfigList.add(newConfig);
            return OK;
        }

        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                shardConfigList.get(lastConfigNum).groupInfo();
        if (groupInfo.containsKey(groupId)) {
            // group id already exist
            return ERROR;
        }

//        newGroupInfo.putAll(groupInfo);

//        List<Integer> curShardsSize = new ArrayList<>();
//        int maxNum = 0;
//        for (int gid: newGroupInfo.keySet()) {
//            int shardSize = newGroupInfo.get(gid).getRight().size();
//            curShardsSize.add(shardSize);
//            if (shardSize > maxNum) {
//                maxNum = shardSize;
//            }
//        }

//        int maxShardNum = maxNum - 1;
//        for (int addShard = 0; addShard < maxShardNum; maxShardNum--) {
//            for (int shardSize: curShardsSize) {
//                if (shardSize - maxShardNum > 0) {
//                    addShard++;
//                }
//            }
//        }
//
//        Set<Integer> newJoinShard = new HashSet<>();
//        for (int gid: newGroupInfo.keySet()) {
//            Set<Integer> shardSet = newGroupInfo.get(gid).getRight();
//            if (shardSet.size() > maxShardNum) {
//                // distribute shards from this group to the joining group
//                Set<Integer> newShards = new HashSet<>(shardSet);
//                int removeShardNum = shardSet.size() - maxShardNum;
//                int i = 0;
//                for (int shard: shardSet) {
//                    if (i >= removeShardNum || newJoinShard.size() >= maxShardNum) {
//                        break;
//                    }
//                    newShards.remove(shard);
//                    newJoinShard.add(shard);
//                    currentGroupInfo[shard] = groupId;
//                    i++;
//                }
//
//                newGroupInfo.put(gid, modifyShards(newGroupInfo.get(gid), newShards));
//            }
//
//            if (newJoinShard.size() >= maxShardNum) {
//                // joining group is full
//                break;
//            }
//        }

        TreeSet<Pair<Integer, Set<Integer>>> groups = new TreeSet<>(
                SIZE_GID_CMP);
        for (Integer gid : groupInfo.keySet()) {
            Set<Integer> clonedShards = new HashSet<>(groupInfo.get(gid).getRight());
            groups.add(p(gid, clonedShards));
        }
        groups.add(p(groupId, new HashSet<>()));
        balance(groups);

        newGroupInfo = makeGroupInfo(groups, groupInfo);
        newGroupInfo.put(groupId, p(servers, newGroupInfo.get(groupId).getRight()));
        ShardConfig newConfig = new ShardConfig(lastConfigNum + 1, newGroupInfo);
        shardConfigList.add(newConfig);
        return OK;
    }

    private Result executeLeave(Leave leave) {
        int groupId = leave.groupId();
        int lastConfigNum = getLastConfigNum();
        if (lastConfigNum < INITIAL_CONFIG_NUM) {
            // before first config
            return ERROR;
        }

        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                shardConfigList.get(lastConfigNum).groupInfo();
        if (!groupInfo.containsKey(groupId) || groupInfo.size() == 1) {
            // leaving group does not exist or only one group left
            return ERROR;
        }

//        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo =
//                new HashMap<>(groupInfo);
//
//        List<Integer> freeShards = new ArrayList<>(
//                groupInfo.get(groupId).getRight());
//        int numFreeShards = freeShards.size();
//        newGroupInfo.remove(groupId);
//
//        List<Integer> curShardsSize = new ArrayList<>();
//        for (int gid: newGroupInfo.keySet()) {
//            curShardsSize.add(newGroupInfo.get(gid).getRight().size());
//        }
//
//        int minShardNum = 1;
//        // TODO: should this be numFreeShards > number of groups?
//        // TODO: do binary search here
//        for (; numFreeShards > 0; minShardNum++) {
//            for (int shardSize: curShardsSize) {
//                if (minShardNum > shardSize) {
//                    numFreeShards--;
//                }
//            }
//        }
//
//        numFreeShards = freeShards.size();
//        int curAssignShards = 0;
//        for (int gid: newGroupInfo.keySet()) {
//            Set<Integer> shardSet = newGroupInfo.get(gid).getRight();
//            if (shardSet.size() < minShardNum) {
//                int addShardNum = minShardNum - shardSet.size();
//                Set<Integer> newShards = new HashSet<>(shardSet);
//                for (int i = 0; i < addShardNum && curAssignShards < numFreeShards; i++) {
//                    int shardIndex = freeShards.get(curAssignShards);
//                    newShards.add(shardIndex);
//                    currentGroupInfo[shardIndex] = gid;
//                    curAssignShards++;
//                }
//                newGroupInfo.put(gid, modifyShards(newGroupInfo.get(gid), newShards));
//            }
//            if (curAssignShards >= numFreeShards) {
//                break;
//            }
//        }

        TreeSet<Pair<Integer, Set<Integer>>> groups = new TreeSet<>(
                SIZE_GID_CMP);
        for (Integer gid : groupInfo.keySet()) {
            if (gid != groupId) {
                Set<Integer> clonedShards = new HashSet<>(groupInfo.get(gid).getRight());
                groups.add(p(gid, clonedShards));
            }
        }
        groups.last().getRight().addAll(groupInfo.get(groupId).getRight());
        balance(groups);

        ShardConfig newConfig = new ShardConfig(lastConfigNum + 1, makeGroupInfo(groups, groupInfo));
        shardConfigList.add(newConfig);
        return OK;
    }

    private Result executeMove(Move move) {
        int groupId = move.groupId();
        int shardNum = move.shardNum();
        int lastConfigNum = getLastConfigNum();
        if (lastConfigNum < INITIAL_CONFIG_NUM || shardNum <= 0 ||
                shardNum > numShards) {
            // before first config or invalid shardNum
            return ERROR;
        }

        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo =
                shardConfigList.get(lastConfigNum).groupInfo();
        if (!groupInfo.containsKey(groupId)) {
            return ERROR;
        }

        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo =
                new HashMap<>(groupInfo);
        int oldGroupId = currentGroupInfo[shardNum];
        if (oldGroupId == groupId) {
            // moving shard to the same group
            return ERROR;
        }

        currentGroupInfo[shardNum] = groupId;

        Pair<Set<Address>, Set<Integer>> fromGid = groupInfo.get(oldGroupId);
        Pair<Set<Address>, Set<Integer>> toGid = groupInfo.get(groupId);

        Set<Integer> newFromShard = new HashSet<>(fromGid.getRight());
        Set<Integer> newToShard = new HashSet<>(toGid.getRight());

        assert(newFromShard.contains(shardNum));
        newFromShard.remove(shardNum);
        newToShard.add(shardNum);

        newGroupInfo.put(oldGroupId, modifyShards(fromGid, newFromShard));
        newGroupInfo.put(groupId, modifyShards(toGid, newToShard));

        ShardConfig newConfig = new ShardConfig(lastConfigNum + 1, newGroupInfo);
        shardConfigList.add(newConfig);
        return OK;
    }

    private Result executeQuery(Query query) {
        if (shardConfigList.isEmpty()) {
            // first config has not been created
            return ERROR;
        }

        int configNum = query.configNum();
        int lastConfigNum = getLastConfigNum();
        if (configNum == -1 || configNum > lastConfigNum) {
            // get last config
            return new ShardConfig(lastConfigNum,
                    shardConfigList.get(lastConfigNum).groupInfo());
        }

        assert(configNum >= 0) : "config number should be >= -1";
        // TODO: clone groupInfo to prevent rep-exposure
        return new ShardConfig(configNum,
                shardConfigList.get(configNum).groupInfo());
    }

    // util
    private int getLastConfigNum() {
        return shardConfigList.size() - 1;
    }

    private Pair<Set<Address>, Set<Integer>> modifyShards(
            Pair<Set<Address>, Set<Integer>> group,
            Set<Integer> shard
    ) {
        return p(group.getLeft(), shard);
    }

    private <L, R> Pair<L, R> p(L left, R right) {
        return new ImmutablePair<>(left, right);
    }

    private Map<Integer, Pair<Set<Address>, Set<Integer>>> makeGroupInfo(
            Set<Pair<Integer, Set<Integer>>> groups,
            Map<Integer, Pair<Set<Address>, Set<Integer>>> oldGroupInfo) {
        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
        for (Pair<Integer, Set<Integer>> group : groups) {
            int gid = group.getLeft();
            Set<Integer> shards = group.getRight();
            Set<Address> groupServers = oldGroupInfo.containsKey(gid)
                    ? oldGroupInfo.get(gid).getLeft()
                    : null;
            groupInfo.put(gid, p(groupServers, shards));
        }
        return groupInfo;
    }

    private void balance(TreeSet<Pair<Integer, Set<Integer>>> groups) {
        while (true) {
            Pair<Integer, Set<Integer>> first = groups.first();
            Pair<Integer, Set<Integer>> last = groups.last();
            Set<Integer> firstShardSet = first.getRight();
            Set<Integer> lastShardSet = last.getRight();

            if (firstShardSet.size() + 1 >= lastShardSet.size()) {
                return;
            }

            groups.remove(first);
            groups.remove(last);
            int shard = lastShardSet.iterator().next();
            lastShardSet.remove(shard);
            firstShardSet.add(shard);
            groups.add(first);
            groups.add(last);
        }
    }
}
