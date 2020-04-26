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
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    // Your code here...
    private AMOCommand amoCommand;
    private Result result;
    private View currentView;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PBClient(Address address, Address viewServer) {
        super(address);
        this.viewServer = viewServer;
    }

    @Override
    public synchronized void init() {
        // Your code here...
        currentView = new View(STARTUP_VIEWNUM, null, null);
        send(new GetView(), viewServer);
        set(new ClientGetViewTimer(), GET_VIEW_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        int sequenceNum = amoCommand == null ? 0 : amoCommand.sequenceNum() + 1;
        amoCommand = new AMOCommand(command, address(), sequenceNum);
        result = null;
        sendCommandToPrimary(amoCommand);
        set(new ClientTimer(amoCommand.sequenceNum()), CLIENT_RETRY_MILLIS);
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
        if (isPrimary(sender) && m.amoResult() != null &&
                amoCommand.sequenceNum() == m.amoResult().sequenceNum()) {
            result = m.amoResult().result();
            notify();
        }
    }

    private synchronized void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if (Objects.equals(sender, viewServer) &&
                currentView.viewNum() < m.view().viewNum()) {
            currentView = m.view();
            if (result == null && amoCommand != null) {
                sendCommandToPrimary(amoCommand);
            }
        }
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (result == null && amoCommand != null &&
                amoCommand.sequenceNum() == t.sequenceNum()) {
            sendCommandToPrimary(amoCommand);
            set(t, CLIENT_RETRY_MILLIS);
        }
    }

    private synchronized void onClientGetViewTimer(ClientGetViewTimer t) {
        send(new GetView(), viewServer);
        set(t, GET_VIEW_MILLIS);
    }

    private void sendCommandToPrimary(AMOCommand amoCommand) {
        if (currentView.primary() != null) {
            send(new Request(amoCommand), currentView.primary());
        }
    }

    private boolean isPrimary(Address address) {
        return Objects.equals(currentView.primary(), address);
    }
}
