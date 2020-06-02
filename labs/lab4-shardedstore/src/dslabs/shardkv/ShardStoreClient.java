package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Error;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.shardmaster.ShardMaster.INITIAL_CONFIG_NUM;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    // Your code here...
    private Result result = null;
    private int seqNum = 0;

    private int configNum;
    private Map<Integer, Set<Address>> shardAssignments;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ShardStoreClient(Address address, Address[] shardMasters,
                            int numShards) {
        super(address, shardMasters, numShards);
    }

    @Override
    public synchronized void init() {
        // Your code here...
        shardAssignments = new HashMap<Integer, Set<Address>>();
        configNum = INITIAL_CONFIG_NUM - 1;
        onQueryTimer(new QueryTimer(0));
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        assert (command instanceof SingleKeyCommand);
        seqNum++;
        AMOCommand amoCommand = new AMOCommand(command, address(), seqNum);
        ShardStoreRequest request = new ShardStoreRequest(amoCommand);
        result = null;
        onClientTimer(new ClientTimer(request));
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return result != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (result == null) {
            wait();
        }
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleShardStoreReply(ShardStoreReply m,
                                                    Address sender) {
        // Your code here...
        AMOResult amoResult = m.amoResult();
        if (amoResult != null && amoResult.sequenceNum() == seqNum) {
            assert (Objects.equals(address(), amoResult.sender()));
            result = m.amoResult().result();
            notify();
        }
    }

    // Your code here...

    private void handlePaxosReply(PaxosReply m, Address sender) {
        // reply from Shard Master (Query result)
        Result result = m.amoResult().result();
        if (result instanceof Error) {
            return;
        }
        assert (result instanceof ShardConfig);

        ShardConfig config = (ShardConfig) result;
        int m_configNum = config.configNum();
        if (m_configNum <= configNum) {
            // old configuration
            return;
        }

        configNum = m_configNum;
        config.groupInfo().forEach((k, v) -> {
            Set<Address> group = new HashSet<>(v.getLeft());
            v.getRight().forEach(shard -> shardAssignments.put(shard, group));
        });
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        AMOCommand amoCommand = t.shardStoreRequest().amoCommand();
        if (amoCommand.sequenceNum() != seqNum || result != null) {
            return;
        }

        if (configNum >= INITIAL_CONFIG_NUM) {
            SingleKeyCommand command = (SingleKeyCommand) amoCommand.command();
            int shard = keyToShard(command.key());
            Set<Address> group = shardAssignments.get(shard);
            broadcast(new ShardStoreRequest(amoCommand), group);
        }
        set(t, ClientTimer.CLIENT_RETRY_MILLIS);
    }

    private void onQueryTimer(QueryTimer t) {
        Query query = new Query(-1);
        AMOCommand command = new AMOCommand(query, address(), true);
        broadcastToShardMasters(new PaxosRequest(command));
        set(t, QueryTimer.QUERY_MILLIS);
    }
}
