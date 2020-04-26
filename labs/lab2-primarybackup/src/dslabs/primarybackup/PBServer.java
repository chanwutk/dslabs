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
import static dslabs.primarybackup.STRequestTimer.ST_REQUEST_MILLIS;
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
    private Role role;

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
        transferredViewNum = -1;
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
//        LOGGER.info("  role: " + role);
//        LOGGER.info("  request: " + m);
//        LOGGER.info("  view: " + currentView);
//        LOGGER.info("  transferring: " + transferring);
//        LOGGER.info("  forwardingCmd: " + forwardingCommand);
        AMOCommand amoCommand = m.amoCommand();
        if (role == Role.PRIMARY) {
            if (amoApplication.alreadyExecuted(amoCommand)) {
                AMOResult amoResult = runAMOCommand(amoCommand);
                send(new Reply(amoResult), sender);
            } else if (!transferring) {
                Address currentBackup = currentView.backup();
                if (currentBackup == null) {
                    forwardingCommand = null;
                    AMOResult amoResult = runAMOCommand(amoCommand);
                    send(new Reply(amoResult), sender);
                } else if (forwardingCommand == null) {
                    forwardingCommand = amoCommand;
                    FRequest fRequest = new FRequest(amoCommand, sender);
                    send(fRequest, currentBackup);
                    set(new FRequestTimer(fRequest,
                            currentBackup), F_REQUEST_MILLIS);
                }
            } else {
                send(REJECT, sender);
            }
        } else {
            send(REJECT, sender);
        }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if (Objects.equals(sender, viewServer) && m.view().viewNum() > currentView.viewNum()) {
            forwardingCommand = null;
            Address prevBackup = currentView.backup();
            currentView = m.view();
            role = getRole(currentView);
            Address backup = currentView.backup();
            if (role == Role.PRIMARY && !Objects.equals(prevBackup, backup)) {
//                 LOGGER.info("handleViewReply");
//                 LOGGER.info("  prev backup: " + prevBackup);
//                 LOGGER.info("  curr backup: " + backup);
//                 LOGGER.info("  curr view: " + currentView);
                if (backup != null) {
//                    LOGGER.info("handleViewReply -> transfer");
//                    LOGGER.info("  prev backup: " + prevBackup);
//                    LOGGER.info("  curr backup: " + backup);
//                    LOGGER.info("  curr view: " + currentView);
                    transferring = true;
                    send(new STRequest(amoApplication, currentView), backup);
                    set(new STRequestTimer(backup), ST_REQUEST_MILLIS);
                }
            }
//            send(new Ping(currentView.viewNum()), viewServer);
        }
    }

    // Your code here...
    private void handleFRequest(FRequest m, Address sender) {
        AMOCommand amoCommand = m.amoCommand();
        boolean accept = false;
        if (role == Role.BACKUP &&
                Objects.equals(sender, currentView.primary())) {
//            LOGGER.info("handleForwardingRequest -> accept");
//            LOGGER.info("  curr address: " + address());
//            LOGGER.info("  from: " + sender);
//            LOGGER.info("  current view: " + currentView);
//            LOGGER.info("  command: " + m.amoCommand());
            runAMOCommand(amoCommand);
            accept = true;
        }
        send(new FReply(accept, amoCommand, m.sender()), sender);
    }

    private void handleFReply(FReply m, Address sender) {
        Reply reply = REJECT;
        AMOCommand amoCommand = m.amoCommand();
        if (role == Role.PRIMARY && forwardingCommand != null &&
                Objects.equals(sender, currentView.backup()) && m.accept() &&
                Objects.equals(amoCommand, forwardingCommand)) {
            AMOResult amoResult = runAMOCommand(amoCommand);
            reply = new Reply(amoResult);
            forwardingCommand = null;
        }
        send(reply, m.sender());
    }

    private void handleSTRequest(STRequest m, Address sender) {
        if (role != Role.BACKUP) {
            send(new STReply(false, m.view()), sender);
            return;
        }

        View view = m.view();
        if (view.viewNum() == transferredViewNum) {
            send(new STReply(true, m.view()), sender);
            return;
        }

        if (view.viewNum() > transferredViewNum) {
            transferredViewNum = view.viewNum();
//            LOGGER.info("handleStateTransferRequest -> transfer");
//            LOGGER.info("  curr address: " + address());
//            LOGGER.info("  transfer view: " + m.view());
//            LOGGER.info("  current  view: " + currentView);
//            LOGGER.info("  same?        : " + sameView(m.view(), currentView));
            amoApplication = m.amoApplication();
        }
        send(new STReply(true, m.view()), sender);
    }

    private void handleSTReply(STReply m, Address sender) {
        if (!m.accept()) {
            transferring = false;
        }

        if (role == Role.PRIMARY && m.view().viewNum() >= currentView.viewNum()) {
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
        if (!transferring && Objects.equals(t.fRequest().amoCommand(), forwardingCommand)) {
            send(t.fRequest(), t.backup());
            set(t, F_REQUEST_MILLIS);
        }
    }

    private void onSTRequestTimer(STRequestTimer t) {
        Address backup = t.backup();
        if (Objects.equals(backup, currentView.backup())) {
            send(new STRequest(amoApplication, currentView), backup);
            set(t, ST_REQUEST_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private Role getRole(View view) {
        Address address = address();
        if (Objects.equals(address, view.primary())) {
            return Role.PRIMARY;
        } else if (Objects.equals(address, view.backup())) {
            return Role.BACKUP;
        }
        return Role.OTHER;
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
