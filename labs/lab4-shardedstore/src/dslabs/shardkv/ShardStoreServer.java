package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosServer;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private static final String PAXOS_ADDRESS_ID = "paxos";

    private final Address[] group;
    private final int groupId;

    // Your code here...
    private final AMOApplication<KVStore> app;
    private Address paxosAddress;
    private int sequenceNum;

    // history of shard config
    private final List<ShardConfig> shardConfigList;

    /* -------------------------------------------------------------------------
        Construction and initialization
       -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;

        // Your code here...
        this.app = new AMOApplication<>(new KVStore());
        shardConfigList = new ArrayList<>();
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

        sequenceNum = 0;
        onQueryTimer(new QueryTimer());
    }


    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        // Your code here...
        AMOCommand amoCommand = m.amoCommand();
        if (app.alreadyExecuted(amoCommand)) {
            send(new ShardStoreReply(app.execute(amoCommand)), sender);
            return;
        }
        PaxosRequest r = new PaxosRequest(amoCommand);
        handleMessage(r, paxosAddress);
        // TODO: finish this
    }

    // Your code here...

    private void handlePaxosReply(PaxosReply m, Address sender) {
        Result result = m.amoResult().result();
        assert (result instanceof ShardConfig);

        ShardConfig config = (ShardConfig) result;
        int configNum = config.configNum();
        assert (configNum <= shardConfigList.size());

        if (configNum == shardConfigList.size()) {
            // new configuration
            shardConfigList.add(config);
            // TODO: reconfigure
        }
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onQueryTimer(QueryTimer t) {
        Query query = new Query(shardConfigList.size());
        AMOCommand command = new AMOCommand(query, address());
        broadcastToShardMasters(new PaxosRequest(command));
        set(t, QueryTimer.QUERY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
}
