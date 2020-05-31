package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    // Your code here...
    private Result result = null;
    private int sequenceNum = 0;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
    }

    @Override
    public synchronized void init() {
        // No need to initialize
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        // Your code here...
        assert(operation != null);

        sequenceNum++;
        AMOCommand amoCommand = new AMOCommand(operation, address(), sequenceNum);
        PaxosRequest paxosRequest = new PaxosRequest(amoCommand);
        result = null;

        broadcast(paxosRequest);
        set(new ClientTimer(paxosRequest), CLIENT_RETRY_MILLIS);
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
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // Your code here...
        AMOResult amoResult = m.amoResult();
        if (amoResult != null && amoResult.sequenceNum() == sequenceNum) {
            assert (Objects.equals(address(), amoResult.sender()));
            result = m.amoResult().result();
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        PaxosRequest paxosRequest = t.paxosRequest();
        if (paxosRequest.amoCommand().sequenceNum() == sequenceNum && result == null) {
            broadcast(paxosRequest);
            set(t, CLIENT_RETRY_MILLIS);
        }
    }

    private void broadcast(@NonNull PaxosRequest paxosRequest) {
        for (Address server : servers) {
            send(paxosRequest, server);
        }
    }
}
