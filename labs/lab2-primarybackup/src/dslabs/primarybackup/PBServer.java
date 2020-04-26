package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import java.util.HashSet;
import java.util.Objects;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.StateTransferTimer.STATE_TRANSFER_MILLIS;
import static dslabs.primarybackup.ForwardingRequestTimer.FORWARDING_REQUEST_MILLIS;
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

    private final Set<AMOCommand> forwardingCommands;
    private AMOApplication<Application> amoApplication;
    private View currentView;
    private Role role;

    private ForwardingRequest curForwardingRequest;
    private ForwardingReply curForwardingReply;

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
        forwardingCommands = new HashSet<>();
        currentView = null;
        transferring = false;
        transferredViewNum = -1;
        LOGGER.setLevel(Level.OFF);
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
        AMOCommand amoCommand = m.amoCommand();
        if (role == Role.PRIMARY) {
            if (amoApplication.alreadyExecuted(amoCommand)) {
                AMOResult amoResult = runAMOCommand(amoCommand);
                send(new Reply(amoResult), sender);
            } else if (!transferring) {
                Address currentBackup = currentView.backup();
                if (currentBackup == null) {
                    AMOResult amoResult = runAMOCommand(amoCommand);
                    send(new Reply(amoResult), sender);
                } else {
                    if (curForwardingRequest == null) {
                        forwardingCommands.add(amoCommand);
                        curForwardingRequest = new ForwardingRequest(amoCommand, sender);
                        send(curForwardingRequest, currentBackup);
                        set(new ForwardingRequestTimer(curForwardingRequest,
                                currentBackup), FORWARDING_REQUEST_MILLIS);
                    }
                }
            }
        } else {
            send(REJECT, sender);
        }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if (Objects.equals(sender, viewServer)) {
            Address prevBackup =
                    currentView == null ? null : currentView.backup();
            currentView = m.view();
            role = getRole(currentView);
            Address backup = currentView.backup();
            if (role == Role.PRIMARY && !Objects.equals(prevBackup, backup)) {
                if (backup != null) {
                    //                    LOGGER.info("handleViewReply -> transfer");
                    //                    LOGGER.info("  prev backup: " + prevBackup);
                    //                    LOGGER.info("  curr backup: " + backup);
                    //                    LOGGER.info("  curr view: " + currentView);
                    transferring = true;
                    send(new StateTransferRequest(amoApplication, currentView),
                            backup);
                    set(new StateTransferTimer(backup), STATE_TRANSFER_MILLIS);
                }
            }
        }
    }

    // Your code here...
    private void handleForwardingRequest(ForwardingRequest m, Address sender) {
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
        send(new ForwardingReply(accept, amoCommand, m.sender()), sender);
    }

    private void handleForwardingReply(ForwardingReply m, Address sender) {
        Reply reply = REJECT;
        if (role == Role.PRIMARY && curForwardingRequest != null &&
                Objects.equals(sender, currentView.backup()) && m.accept()) {
            AMOCommand amoCommand = m.amoCommand();
            AMOResult amoResult = runAMOCommand(amoCommand);
            forwardingCommands.remove(amoCommand);
            reply = new Reply(amoResult);
        }
        curForwardingRequest = null;
        send(reply, m.sender());
    }

    private void handleStateTransferRequest(StateTransferRequest m,
                                            Address sender) {
        View view = m.view();
        if (view.viewNum() > transferredViewNum) {
            transferredViewNum = view.viewNum();
            //            LOGGER.info("handleStateTransferRequest -> transfer");
            //            LOGGER.info("  curr address: " + address());
            //            LOGGER.info("  transfer view: " + m.view());
            //            LOGGER.info("  current  view: " + currentView);
            //            LOGGER.info("  same?        : " + sameView(m.view(), currentView));
            //            if (m.amoCommands().size() > 0) {
            //                LOGGER.info("  last request: " +
            //                        m.amoCommands().get(m.amoCommands().size() - 1));
            //            }
            amoApplication = m.amoApplication();
            send(new StateTransferReply(true, m.view()), sender);
        }
    }

    private void handleStateTransferReply(StateTransferReply m,
                                          Address sender) {
        if (role == Role.PRIMARY && sameView(m.view(), currentView)) {
            transferring = false;
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...

    private void onPingTimer(PingTimer t) {
        // Your code here...
        int viewNum =
                currentView == null ? STARTUP_VIEWNUM : currentView.viewNum();
        send(new Ping(viewNum), viewServer);
        set(t, PING_CHECK_MILLIS);
    }

    private void onForwardingRequestTimer(ForwardingRequestTimer t) {
        if (!transferring && forwardingCommands.contains(t.forwardingRequest().amoCommand())) {
            send(t.forwardingRequest(), t.backup());
            set(t, FORWARDING_REQUEST_MILLIS);
        }
    }

    private void onStateTransferTimer (StateTransferTimer t) {
        Address backup = t.backup();
        if (Objects.equals(backup, currentView.backup())) {
            send(new StateTransferRequest(amoApplication, currentView), backup);
            set(t, STATE_TRANSFER_MILLIS);
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
