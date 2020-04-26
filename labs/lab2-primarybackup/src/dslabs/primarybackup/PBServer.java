package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import java.util.Objects;

import java.util.logging.Logger;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.STRequestTimer.S_T_REQUEST_MILLIS;
import static dslabs.primarybackup.FRequestTimer.F_REQUEST_MILLIS;
import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;

    // Your code here...
    private static final Reply REJECT = new Reply(null);
    private static final Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    private AMOApplication<Application> amoApplication;
    private View currentView;
    private boolean isPrimary;
    private boolean isBackup;

    private AMOCommand forwardingCommand;

    private boolean transferring;
    private int transferredViewNum;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.viewServer = viewServer;

        // Your code here...
        amoApplication = new AMOApplication<>(app);
        currentView = new View(STARTUP_VIEWNUM, null, null);
        transferring = false;
        forwardingCommand = null;
        transferredViewNum = -1;
        isPrimary = false;
        isBackup = false;
        // LOGGER.setLevel(Level.OFF);
    }

    @Override
    public void init() {
        // Your code here...
        send(new Ping(STARTUP_VIEWNUM), viewServer);
        set(new PingTimer(), PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender) {
        // Your code here...
//        LOGGER.info("handleRequest");
//        LOGGER.info("  address: " + address());
//        LOGGER.info("  client: " + sender);
//        LOGGER.info("  role: " + isPrimary + " " + isBackup);
//        LOGGER.info("  request: " + m);
//        LOGGER.info("  view: " + currentView);
//        LOGGER.info("  transferring: " + transferring);
//        LOGGER.info("  forwardingCmd: " + forwardingCommand);
        AMOCommand amoCommand = m.amoCommand();
        if (!isPrimary || transferring) {
            send(REJECT, sender);
            return;
        }

        if (amoApplication.alreadyExecuted(amoCommand)) {
            AMOResult amoResult = runAMOCommand(amoCommand);
            send(new Reply(amoResult), sender);
            return;
        }

        Address backup = currentView.backup();
        if (backup == null) {
            forwardingCommand = null;
            AMOResult amoResult = runAMOCommand(amoCommand);
            send(new Reply(amoResult), sender);
        } else if (forwardingCommand == null) {
            forwardingCommand = amoCommand;
            FRequest fRequest = new FRequest(amoCommand, sender);
            send(fRequest, backup);
            set(new FRequestTimer(fRequest, backup), F_REQUEST_MILLIS);
        }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if (isViewServer(sender) && m.view().viewNum() > currentView.viewNum()) {
            Address prevBackup = currentView.backup();
            Address address = address();

            forwardingCommand = null;
            currentView = m.view();
            isPrimary = isPrimary(address);
            isBackup = isBackup(address);
            Address backup = currentView.backup();
            if (isPrimary && backup != null && !isBackup(prevBackup)) {
//                LOGGER.info("handleViewReply -> transfer");
//                LOGGER.info("  prev backup: " + prevBackup);
//                LOGGER.info("  curr backup: " + backup);
//                LOGGER.info("  curr view: " + currentView);
                transferring = true;
                send(new STRequest(amoApplication, currentView), backup);
                set(new STRequestTimer(backup), S_T_REQUEST_MILLIS);
            }
        }
    }

    // Your code here...
    private void handleFRequest(FRequest m, Address sender) {
        AMOCommand amoCommand = m.amoCommand();
        if (isBackup || !isPrimary(sender)) {
            send(new FReply(false, amoCommand, m.sender()), sender);
        }

//        LOGGER.info("handleForwardingRequest -> accept");
//        LOGGER.info("  curr address: " + address());
//        LOGGER.info("  from: " + sender);
//        LOGGER.info("  current view: " + currentView);
//        LOGGER.info("  command: " + m.amoCommand());
        runAMOCommand(amoCommand);
        send(new FReply(true, amoCommand, m.sender()), sender);
    }

    private void handleFReply(FReply m, Address sender) {
        AMOCommand amoCommand = m.amoCommand();
        if (!isPrimary || !m.accept() || !isBackup(sender) || !isForwarding(amoCommand)) {
            send(REJECT, m.sender());
        }

        AMOResult amoResult = runAMOCommand(amoCommand);
        forwardingCommand = null;
        send(new Reply(amoResult), m.sender());
    }

    private void handleSTRequest(STRequest m, Address sender) {
        View view = m.view();
        if (view.viewNum() <= transferredViewNum) {
            return;
        }

        transferredViewNum = view.viewNum();
//        LOGGER.info("handleStateTransferRequest -> transfer");
//        LOGGER.info("  curr address: " + address());
//        LOGGER.info("  transfer view: " + m.view());
//        LOGGER.info("  current  view: " + currentView);
//        LOGGER.info("  same?        : " + sameView(m.view(), currentView));
        amoApplication = m.amoApplication();
        send(new STReply(m.view()), sender);
    }

    private void handleSTReply(STReply m, Address sender) {
        if (isPrimary && sameView(m.view(), currentView)) {
            transferring = false;
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...

    private void onPingTimer(PingTimer t) {
        // Your code here...
        int viewNum = currentView.viewNum();
        send(new Ping(viewNum), viewServer);
        set(t, PING_MILLIS);
    }

    private void onFRequestTimer(FRequestTimer t) {
        if (!transferring && isForwarding(t.fRequest().amoCommand())) {
            send(t.fRequest(), t.backup());
            set(t, F_REQUEST_MILLIS);
        }
    }

    private void onSTRequestTimer (STRequestTimer t) {
        Address backup = t.backup();
        if (isBackup(backup)) {
            send(new STRequest(amoApplication, currentView), backup);
            set(t, S_T_REQUEST_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private boolean isForwarding(AMOCommand amoCommand) {
        return Objects.equals(amoCommand, forwardingCommand);
    }

    private boolean isViewServer(Address address) {
        return Objects.equals(address, viewServer);
    }

    private boolean isPrimary(Address address) {
        return Objects.equals(address, currentView.primary());
    }

    private boolean isBackup(Address address) {
        return Objects.equals(address, currentView.backup());
    }

    private AMOResult runAMOCommand(AMOCommand amoCommand) {
        return amoApplication.execute(amoCommand);
    }

    private boolean sameView(View view1, View view2) {
        return view1.viewNum() == view2.viewNum();
    }

    private enum Role {
        PRIMARY,
        BACKUP,
        OTHER,
    }
}
