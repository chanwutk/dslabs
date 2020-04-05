package dslabs.clientserver;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.clientserver.ClientTimer.CLIENT_RETRY_MILLIS;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * See the documentation of {@link Client} and {@link Node} for important
 * implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
    private final Address serverAddress;

    // Your code here...
    private Address address;
    private AMOCommand amoCommand;
    private Result result;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public SimpleClient(Address address, Address serverAddress) {
        super(address);
        this.address = address;
        this.serverAddress = serverAddress;
    }

    @Override
    public synchronized void init() {
        // No initialization necessary
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        int sequenceNum = this.amoCommand == null ?
                0 : this.amoCommand.sequenceNum() + 1;
        this.amoCommand = new AMOCommand(command, address, sequenceNum);
        result = null;

        send(new Request(this.amoCommand), serverAddress);
        set(new ClientTimer(this.amoCommand), CLIENT_RETRY_MILLIS);
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
    private synchronized void handleReply(Reply m, Address sender) {
        // Your code here...
        if (amoCommand.sequenceNum() == m.amoResult().sequenceNum()) {
            result = m.amoResult().result();
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (result == null && amoCommand != null &&
                amoCommand.sequenceNum() == t.command().sequenceNum()) {
            send(new Request(amoCommand), serverAddress);
            set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
