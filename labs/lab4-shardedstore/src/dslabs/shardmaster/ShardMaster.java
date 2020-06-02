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
import java.util.Stack;
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
    private static final Comparator<Pair<Integer, Stack<Integer>>> SIZE_CMP =
            Comparator.comparingInt(e -> e.getRight().size());
    private static final Comparator<Pair<Integer, Stack<Integer>>> SIZE_GID_CMP =
            SIZE_CMP.thenComparingInt(Pair::getLeft);
    private static final Result OK = new Ok();
    private static final Result ERROR = new Error();

    // history of shard config
    private final List<ShardConfig> shardConfigList;
    // assignment of each shard (shard number starts at 1)
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

        // clone every set of shards in each group
        TreeSet<Pair<Integer, Stack<Integer>>> groups = new TreeSet<>(SIZE_GID_CMP);
        groupInfo.forEach((gid, info) -> {
            Stack<Integer> stack = new Stack<>();
            stack.addAll(info.getRight());
            groups.add(p(gid, stack));
        });

        // add the new group
        groups.add(p(groupId, new Stack<>()));

        // shards are now not evenly distributed -> re-balance them
        balance(groups);

        // format for shard config
        newGroupInfo = makeGroupInfo(groups, groupInfo);

        // add list of server to the newly added group
        newGroupInfo.put(groupId, p(servers, newGroupInfo.get(groupId).getRight()));

        newConfig(newGroupInfo);
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

        // clone every set of shards in each group
        TreeSet<Pair<Integer, Stack<Integer>>> groups = new TreeSet<>(SIZE_GID_CMP);
        groupInfo.forEach((gid, info) -> {
            if (gid != groupId) {
                // exclude the leaving group
                Stack<Integer> stack = new Stack<>();
                stack.addAll(info.getRight());
                groups.add(p(gid, stack));
            }
        });

        // add shard of the leaving group to the group that holds the most shard
        groups.last().getRight().addAll(groupInfo.get(groupId).getRight());

        // shards are now not evenly distributed -> re-balance them
        balance(groups);

        newConfig(makeGroupInfo(groups, groupInfo));
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

        int oldGroupId = currentGroupInfo[shardNum];
        if (oldGroupId == groupId) {
            // moving shard to the same group
            return ERROR;
        }

        currentGroupInfo[shardNum] = groupId;

        Pair<Set<Address>, Set<Integer>> fromGid = groupInfo.get(oldGroupId);
        Pair<Set<Address>, Set<Integer>> toGid = groupInfo.get(groupId);

        // clone the sets of shards
        Set<Integer> newFromShard = new HashSet<>(fromGid.getRight());
        Set<Integer> newToShard = new HashSet<>(toGid.getRight());

        assert(newFromShard.contains(shardNum));
        newFromShard.remove(shardNum);
        newToShard.add(shardNum);

        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo =
                new HashMap<>(groupInfo);
        newGroupInfo.put(oldGroupId, modifyShards(fromGid, newFromShard));
        newGroupInfo.put(groupId, modifyShards(toGid, newToShard));

        newConfig(newGroupInfo);
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
            return getShardConfig(lastConfigNum);
        }

        assert(configNum >= 0) : "config number should be >= 0";
        return getShardConfig(configNum);
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
            Set<Pair<Integer, Stack<Integer>>> groups,
            Map<Integer, Pair<Set<Address>, Set<Integer>>> oldGroupInfo
    ) {
        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
        groups.forEach(group -> {
            int gid = group.getLeft();
            Stack<Integer> shards = group.getRight();
            Set<Address> groupServers = oldGroupInfo.containsKey(gid)
                    ? oldGroupInfo.get(gid).getLeft()
                    : null;
            groupInfo.put(gid, p(groupServers, new HashSet<>(shards)));
        });
        return groupInfo;
    }

    private void balance(TreeSet<Pair<Integer, Stack<Integer>>> groups) {
        while (true) {
            Pair<Integer, Stack<Integer>> first = groups.first();
            Pair<Integer, Stack<Integer>> last = groups.last();
            Stack<Integer> firstShardSet = first.getRight();
            Stack<Integer> lastShardSet = last.getRight();

            if (firstShardSet.size() + 1 >= lastShardSet.size()) {
                return;
            }

            groups.remove(first);
            groups.remove(last);
            int shard = lastShardSet.pop();
//            lastShardSet.remove(shard);
            firstShardSet.push(shard);
            groups.add(first);
            groups.add(last);
        }
    }

    private void newConfig(Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo) {
        ShardConfig newConfig = new ShardConfig(shardConfigList.size(), groupInfo);
        shardConfigList.add(newConfig);
    }

    private ShardConfig getShardConfig(int configNum) {
        Map<Integer, Pair<Set<Address>, Set<Integer>>> clonedGroupInfo =
                new HashMap<>();

        shardConfigList.get(configNum).groupInfo().forEach((key, value) -> {
            Set<Address> newServers = new HashSet<>(value.getLeft());
            Set<Integer> newShards = new HashSet<>(value.getRight());
            clonedGroupInfo.put(key, new ImmutablePair<>(newServers, newShards));
        });
        return new ShardConfig(configNum, clonedGroupInfo);
    }
}
