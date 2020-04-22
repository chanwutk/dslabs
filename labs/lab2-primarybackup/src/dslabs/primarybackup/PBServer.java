package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    private static final Reply REJECT = new Reply(false, null);
    private static final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    private final AMOApplication<Application> amoApplication;
    private final Map<AMOCommand, AMOResult> amoResults;
    private final List<AMOCommand> amoCommands;
    private View currentView;
    private Role role;

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
        amoCommands = new ArrayList<>();
        amoResults = new HashMap<>();
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
        LOGGER.info("handleRequest");
        LOGGER.info("  address: " + address());
        LOGGER.info("  client: " + sender);
        LOGGER.info("  role: " + role);
        LOGGER.info("  request: " + m);
        AMOCommand amoCommand = m.amoCommand();
        if (role == Role.PRIMARY && !transferring) {
            if (!amoApplication.alreadyExecuted(amoCommand)) {
                amoCommands.add(amoCommand);
            }
            AMOResult amoResult = runAMOCommand(amoCommand);
            Address currentBackup = currentView.backup();
            if (currentBackup == null) {
                send(new Reply(true, amoResult), sender);
            } else {
                amoResults.put(amoCommand, amoResult);
                ForwardingRequest forwardingRequest =
                        new ForwardingRequest(amoCommand, sender);
                send(forwardingRequest, currentBackup);
                set(new ForwardingRequestTimer(forwardingRequest,
                        currentBackup), FORWARDING_REQUEST_MILLIS);
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
            role = getCurrentRole(currentView);
            Address backup = currentView.backup();
            if (role == Role.PRIMARY && !Objects.equals(prevBackup, backup) &&
                    backup != null) {
                LOGGER.info("handleViewReply -> transfer");
                LOGGER.info("  prev backup: " + prevBackup);
                LOGGER.info("  curr backup: " + backup);
                LOGGER.info("  curr view: " + currentView);
                for (AMOResult amoResult : amoResults.values()) {
                    send(new Reply(true, amoResult), amoResult.sender());
                }
                amoResults.clear();
                transferring = true;
                send(new StateTransferRequest(amoCommands, currentView),
                        backup);
                set(new StateTransferTimer(backup), STATE_TRANSFER_MILLIS);
            }
        }
    }

    // Your code here...
    private void handleForwardingRequest(ForwardingRequest m, Address sender) {
        AMOCommand amoCommand = m.amoCommand();
        if (role == Role.BACKUP &&
                Objects.equals(sender, currentView.primary())) {
            LOGGER.info("handleForwardingRequest -> accept");
            LOGGER.info("  curr address: " + address());
            LOGGER.info("  from: " + sender);
            LOGGER.info("  current view: " + currentView);
            LOGGER.info("  command: " + m.amoCommand());
            amoCommands.add(amoCommand);
            runAMOCommand(amoCommand);
            send(new ForwardingReply(true, amoCommand, m.sender()), sender);
        } else {
            send(new ForwardingReply(false, amoCommand, m.sender()), sender);
        }
    }

    private void handleForwardingReply(ForwardingReply m, Address sender) {
        if (role == Role.PRIMARY &&
                Objects.equals(sender, currentView.backup())) {
            if (m.accept()) {
                AMOResult amoResult = amoResults.remove(m.amoCommand());
                if (amoResult != null) {
                    send(new Reply(true, amoResult), m.sender());
                }
            } else {
                send(REJECT, m.sender());
            }
        } else {
            send(REJECT, m.sender());
        }
    }

    private void handleStateTransferRequest(StateTransferRequest m,
                                            Address sender) {
        View view = m.view();
        if (view.viewNum() > transferredViewNum) {
            transferredViewNum = view.viewNum();
            LOGGER.info("handleStateTransferRequest -> transfer");
            LOGGER.info("  curr address: " + address());
            LOGGER.info("  transfer view: " + m.view());
            LOGGER.info("  current  view: " + currentView);
            LOGGER.info("  same?        : " + sameView(m.view(), currentView));
            if (m.amoCommands().size() > 0) {
                LOGGER.info("  last request: " +
                        m.amoCommands().get(m.amoCommands().size() - 1));
            }
            amoCommands.clear();
            for (AMOCommand amoCommand : m.amoCommands()) {
                amoApplication.execute(amoCommand);
                amoCommands.add(amoCommand);
            }
            send(new StateTransferReply(true, m.view()), sender);
        } else {
            send(new StateTransferReply(false, m.view()), sender);
        }
    }

    private void handleStateTransferReply(StateTransferReply m,
                                          Address sender) {
        if (role == Role.PRIMARY && sameView(m.view(), currentView) &&
                m.accept()) {
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
        if (amoResults.containsKey(t.forwardingRequest().amoCommand())) {
            send(t.forwardingRequest(), t.backup());
            set(t, FORWARDING_REQUEST_MILLIS);
        }
    }

    private void onStateTransferTimer (StateTransferTimer t) {
        Address backup = t.backup();
        if (Objects.equals(backup, currentView.backup())) {
            send(new StateTransferRequest(amoCommands, currentView), backup);
            set(t, STATE_TRANSFER_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private Role getCurrentRole(View currentView) {
        Address address = address();
        if (Objects.equals(address, currentView.primary())) {
            return Role.PRIMARY;
        } else if (Objects.equals(address, currentView.backup())) {
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
