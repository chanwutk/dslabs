package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosDecision;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosServer;
import dslabs.shardmaster.ShardMaster.Error;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import static dslabs.shardmaster.ShardMaster.INITIAL_CONFIG_NUM;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private static final String PAXOS_ADDRESS_ID = "paxos";

    private final Address[] group;
    private final int groupId;

    // Your code here...
    private static final Logger LOG = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private final Map<Integer, AMOApplication<KVStore>> apps;
    private Address paxosAddress;
    private int sequenceNum;
    private Map<Integer, Set<Integer>> waitedAck;
    private ShardConfig config;

    /* -------------------------------------------------------------------------
        Construction and initialization
       -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;

        // Your code here...
        this.apps = new HashMap<>();
    }

    @Override
    public void init() {
        // Your code here...
        // Setup Paxos
        paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

        Address[] paxosAddresses = new Address[group.length];
        for (int i = 0; i < paxosAddresses.length; i++) {
            paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
        }

        PaxosServer paxosServer = new PaxosServer(paxosAddress, paxosAddresses, address());
        addSubNode(paxosServer);
        paxosServer.init();

        // Setup ShardStoreServer
        sequenceNum = 0;
        config = new ShardConfig(-1 , null);
        waitedAck = new HashMap<>();
        onQueryTimer(new QueryTimer(0));
    }


    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        // Your code here...
        process(m.amoCommand(), false, false);
    }

    // Your code here...
    private void handlePaxosReply(PaxosReply m, Address sender) {
        // from ShardMaster
        if (m.amoResult() == null) {
            return;
        }

        if (m.amoResult().result() instanceof Error) {
            return;
        }

        // from shard master
        if (!correctShards()) {
            return;
        }

        Result result = m.amoResult().result();
        process(new NewConfig((ShardConfig) result), false, false);
    }

    private void handleShardMoveMessage(ShardMoveMessage m, Address sender) {
        process(m.command(), false /* It’s not replicated yet */, false);
    }

    private void handleShardMoveAckMessage(ShardMoveAckMessage m, Address sender) {
        process(m.command(), false /* It’s not replicated yet */, false);
    }

    private void handlePaxosDecision(PaxosDecision p, Address sender) {
        process(p.command(), true /* It’s replicated (from Paxos) */, p.toResponse());
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onQueryTimer(QueryTimer t) {
        Query query = new Query(config.configNum() + 1);
        AMOCommand command = new AMOCommand(query, address(), true);
        if (correctShards()) {
            broadcastToShardMasters(new PaxosRequest(command));
        }
        set(t, QueryTimer.QUERY_MILLIS);
    }

    private void onShardMoveTimer(ShardMoveTimer t) {
        int configNum = t.message().command().configNum();
        Set<Integer> shards = t.message().command().apps().keySet();
        if (config.configNum() == configNum && containsSomeShards(shards)) {
            broadcast(t.message(), t.dests());
            set(t, ShardMoveTimer.SHARD_MOVE_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private boolean containsSomeShards(Set<Integer> shards) {
        for (int shard : shards) {
            if (apps.containsKey(shard)) {
                return true;
            }
        }
        return false;
    }

    private void process(Command command, boolean replicated, boolean toReply) {
        // TODO: remove toReply -> broadcast

        if (command instanceof ShardMove) {
            processShardMove((ShardMove) command, replicated, toReply);
        } else if (command instanceof ShardMoveAck) {
            processShardMoveAck((ShardMoveAck) command, replicated, toReply);
        } else if (command instanceof NewConfig) {
            processNewConfig((NewConfig) command, replicated, toReply);
        } else if (command instanceof AMOCommand) {
            processAMOCommand((AMOCommand) command, replicated, toReply);
        }
        // Add cases for Lab 4 Part 3
        else {
            LOG.severe("Got unknown command: " + command);
        }
    }

    private void processShardMove(ShardMove m, boolean replicated, boolean toReply) {
//        String log = "\nshard move: " + m.apps().keySet() + "\n";
//        log += "  " + groupId + " from: " + m.sender() + "\n";
//        log += "  " + groupId + " replicated: " + replicated + "\n";
//        log += "  " + groupId + " toReply: " + toReply + "\n";
//        log += "  " + groupId + " configNum: " + m.configNum() + "\n";
//        log += "  " + groupId + " current config: " + config + "\n";
//        Set<Integer> tmp = config.groupInfo().containsKey(groupId) ? config.groupInfo().get(groupId).getRight() : null;
//        log += "  " + groupId + " shard compare: " + tmp + " " + apps.keySet() + "\n";
//        LOG.info(log);

        if (m.configNum() != config.configNum()) {
            return;
        }

        if (!replicated) {
            paxosPropose(m);
            return;
        }

        ShardMoveAck ack = new ShardMoveAck(m.apps().keySet(), m.configNum());
        if (m.configNum() < config.configNum()) {
            // old shard move
            if (toReply) {
                send(new ShardMoveAckMessage(ack), m.sender());
            }
            return;
        }

        m.apps().forEach((shard, app) -> {
            if (!apps.containsKey(shard)) {
                apps.put(shard, app);
            }
        });

        if (toReply) {
            send(new ShardMoveAckMessage(ack), m.sender());
        }
    }

    private void processShardMoveAck(ShardMoveAck m, boolean replicated, boolean toReply) {
        if (m.configNum() != config.configNum()) {
            return;
        }

        if (!replicated) {
            paxosPropose(m);
            return;
        }

        m.shards().forEach(apps::remove);
    }

    private void processNewConfig(NewConfig m, boolean replicated, boolean toReply) {
//        LOG.info("new config " + groupId +": " + m);
//        LOG.info("  " + groupId + "  config: " + config);
//        LOG.info("  " + groupId + "  replicated: " + replicated);
//        LOG.info("  " + groupId + "  toReply: " + toReply);
//        LOG.info("  " + groupId + "  shards: " + apps.keySet());
        if (m.config().configNum() != config.configNum() + 1) {
            return;
        }

        if (!replicated) {
            paxosPropose(m);
            return;
        }

        config = m.config();
        if (config.configNum() == INITIAL_CONFIG_NUM) {
            // first config
            if (config.groupInfo().containsKey(groupId)) {
                config.groupInfo().get(groupId).getRight()
                      .forEach(shard -> apps.put(shard, new AMOApplication<>(new KVStore())));
            }
            return;
        }

        if (!toReply) {
            return;
        }

        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = config.groupInfo();

        Map<Integer, Set<Integer>> toSend = new HashMap<>();
        Set<Integer> shards = apps.keySet();
        groupInfo.forEach((gid, info) -> {
            if (gid == groupId) {
                return;
            }
            info.getRight().forEach(shard -> {
                if (shards.contains(shard)) {
                    if (!toSend.containsKey(gid)) {
                        toSend.put(gid, new HashSet<>());
                    }
                    toSend.get(gid).add(shard);
                }
            });
        });

        int configNum = config.configNum();
        toSend.forEach((gid, shardsToSend) -> {
            Map<Integer, AMOApplication<KVStore>> appsToSend = new HashMap<>();
            shardsToSend.forEach(shard -> {
                appsToSend.put(shard, apps.get(shard));
            });
            ShardMove shardMove = new ShardMove(appsToSend, configNum, address());
            Set<Address> servers = groupInfo.get(gid).getLeft();
            onShardMoveTimer(new ShardMoveTimer(new ShardMoveMessage(shardMove), servers));
        });
    }

    private void processAMOCommand(AMOCommand command, boolean replicated, boolean toReply) {
        if (!correctShards()) {
            send(new ShardStoreReply(null), command.sender());
            return;
        }

        int shard = amoCommandToShard(command);
        if (!apps.containsKey(shard)) {
            send(new ShardStoreReply(null), command.sender());
            return;
        }

        AMOApplication<KVStore> app = apps.get(shard);
        if (app.alreadyExecuted(command)) {
            send(new ShardStoreReply(app.execute(command)), command.sender());
            return;
        }

        if (!replicated) {
            paxosPropose(command);
            return;
        }

        send(new ShardStoreReply(app.execute(command)), command.sender());
    }

    private void paxosPropose(Command command) {
        handleMessage(new PaxosRequest(new AMOCommand(command, address(), false)), paxosAddress);
    }

    private int amoCommandToShard(AMOCommand amoCommand) {
        return keyToShard(((SingleKeyCommand) amoCommand.command()).key());
    }

    private boolean correctShards() {
        if (config.groupInfo() == null) {
            return apps.isEmpty();
        }

        if (!config.groupInfo().containsKey(groupId)) {
            return apps.isEmpty();
        }
        return config.groupInfo().get(groupId).getRight().equals(apps.keySet());
    }
}
