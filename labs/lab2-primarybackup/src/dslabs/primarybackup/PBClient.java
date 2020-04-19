package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.ClientGetViewTimer.GET_VIEW_MILLIS;
import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    // Your code here...
    private Address address;
    private AMOCommand amoCommand;
    private Result result;
    private View currentView;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PBClient(Address address, Address viewServer) {
        super(address);
        this.address = address;
        this.viewServer = viewServer;
    }

    @Override
    public synchronized void init() {
        // Your code here...
        send(new GetView(), viewServer);
        set(new ClientGetViewTimer(), GET_VIEW_MILLIS);
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
        //System.out.println(currentView);
        if (currentView != null && currentView.primary() != null) {
            send(new Request(this.amoCommand), currentView.primary());
        }
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
        // System.out.println(m);
         if (m.accept() && amoCommand.sequenceNum() == m.amoResult().sequenceNum()) {
            result = m.amoResult().result();
            notify();
        }
    }

    private synchronized void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if (Objects.equals(sender, viewServer)) {
            currentView = m.view();
        }
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (result == null && amoCommand != null &&
                amoCommand.sequenceNum() == t.command().sequenceNum()) {
            if (currentView != null && currentView.primary() != null) {
                send(new Request(amoCommand), currentView.primary());
            }
            set(t, CLIENT_RETRY_MILLIS);
        }
    }

    private synchronized void onClientGetViewTimer(ClientGetViewTimer t) {
        send(new GetView(), viewServer);
        set(t, GET_VIEW_MILLIS);
    }
}
