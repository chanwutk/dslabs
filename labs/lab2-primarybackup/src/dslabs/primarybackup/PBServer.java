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

import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.StateTransferTimer.STATE_TRANSFER_MILLIS;
import static dslabs.primarybackup.ForwardingRequestTimer.FORWARDING_REQUEST_MILLIS;
import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;

    // Your code here...
    private final AMOApplication amoApplication;
    private View currentView;
    private Role role;
    private List<AMOCommand> amoCommands;
    private Map<AMOCommand, AMOResult> amoResults;
    private Address transferringAddress;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.viewServer = viewServer;

        // Your code here...
        amoApplication = new AMOApplication(app);
        amoCommands = new ArrayList<>();
        amoResults = new HashMap<>();
        currentView = null;
        transferringAddress = null;
    }

    @Override
    public void init() {
        // Your code here...
        send(new Ping(ViewServer.STARTUP_VIEWNUM), viewServer);
        set(new PingTimer(), PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender) {
        // Your code here...
        // System.out.println(role + " " + m);
        if (role == Role.PRIMARY && transferringAddress == null) {
            AMOCommand amoCommand = m.amoCommand();
            amoCommands.add(amoCommand);
            AMOResult amoResult = runAMOCommand(amoCommand);
            Address currentBackup = currentView.backup();
            if (currentBackup == null) {
                send(new Reply(true, amoResult), sender);
            } else {
                amoResults.put(amoCommand, amoResult);
                ForwardingRequest forwardingRequest = new ForwardingRequest(amoCommand, sender);
                send(forwardingRequest, currentBackup);
                set(new ForwardingRequestTimer(forwardingRequest, currentBackup),
                        FORWARDING_REQUEST_MILLIS);
            }
        } else {
            send(new Reply(false, null), sender);
        }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if (Objects.equals(sender, viewServer)) {
            Address prevBackup = currentView == null ? null : currentView.backup();
            currentView = m.view();
            role = getCurrentRole(currentView);
            Address backup = currentView.backup();
            if (Objects.equals(prevBackup, backup) && backup != null) {
                transferringAddress = backup;
                send(new StateTransferRequest(amoCommands), backup);
                // send(new StateTransferRequest(amoApplication), backup);
                set(new StateTransferTimer(backup), STATE_TRANSFER_MILLIS);
            }
        }
    }

    // Your code here...
    private void handleForwardingRequest(ForwardingRequest m, Address sender) {
        if (role == Role.BACKUP && Objects.equals(sender, currentView.primary())) {
            AMOCommand amoCommand = m.amoCommand();
            amoCommands.add(amoCommand);
            runAMOCommand(amoCommand);
            send(new ForwardingReply(true, amoCommand, m.sender()), sender);
        } else {
            send(new ForwardingReply(false, null, null), sender);
        }
    }

    private void handleForwardingReply(ForwardingReply m, Address sender) {
        if (role == Role.PRIMARY && Objects.equals(sender, currentView.backup()) && m.accept()) {
            AMOResult amoResult = amoResults.remove(m.amoCommand());
            send(new Reply(true, amoResult), m.sender());
        }
    }

    private void handleStateTransferRequest(StateTransferRequest m, Address sender) {
        if (role == Role.BACKUP) {
            System.out.println(m);
            amoCommands = new ArrayList();
            for (AMOCommand amoCommand : m.amoCommands()) {
                amoApplication.execute(amoCommand);
                amoCommands.add(amoCommand);
            }
            // amoApplication = m.amoApplication().copy();
            send(new StateTransferReply(true), sender);
        } else  {
            send(new StateTransferReply(false), sender);
        }
    }

    private void handleStateTransferReply(StateTransferReply m, Address sender) {
        if (role == Role.PRIMARY) {
            if (m.accept()) {
                transferringAddress = null;
            }
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...

    private void onPingTimer(PingTimer t) {
        // Your code here...
        send(new Ping(currentView.viewNum()), viewServer);
        set(t, PING_CHECK_MILLIS);
    }

    private void onForwardingRequestTimer(ForwardingRequestTimer t) {
        if (amoResults.containsKey(t.forwardingRequest())) {
            send(t.forwardingRequest(), t.backup());
            set(t, FORWARDING_REQUEST_MILLIS);
        }
    }

    private void onStateTransferTimer (StateTransferTimer t) {
        Address backup = t.backup();
        if (Objects.equals(backup, transferringAddress)) {
            send(new StateTransferRequest(amoCommands), backup);
            // send(new StateTransferRequest(amoApplication), backup);
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


    private enum Role {
        PRIMARY,
        BACKUP,
        OTHER,
    }
}
