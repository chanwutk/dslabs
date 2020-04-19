package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
    static final int STARTUP_VIEWNUM = 0;
    private static final int INITIAL_VIEWNUM = 1;

    // Your code here...
    private View currentView;
    private View tentativeView;
    private Set<Address> aliveServers;
    private boolean ack;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ViewServer(Address address) {
        super(address);
    }

    @Override
    public void init() {
        set(new PingCheckTimer(), PING_CHECK_MILLIS);
        // Your code here...
        currentView = new View(STARTUP_VIEWNUM, null, null);
        tentativeView = null;
        aliveServers = new HashSet<>();
        ack = false;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // Your code here...
//        System.out.println(sender + " " + m.viewNum() + " " + currentView.viewNum());
        aliveServers.add(sender);

        if (Objects.equals(sender, currentView.primary()) && m.viewNum() == currentView.viewNum()) {
            if (tentativeView == null) {
                ack = true;
            } else {
                currentView = tentativeView;
                tentativeView = null;
                ack = false;
            }
        }

        Address primary = currentView.primary();
        if (currentView.viewNum() == STARTUP_VIEWNUM) {
            currentView = new View(INITIAL_VIEWNUM, sender, null);
            ack = false;
        } else if (currentView.backup() == null && !Objects.equals(sender, primary)) {
            if (ack) {
                updateView(primary, sender);
            } else {
                tentativeView = new View(nextViewNum(), primary, sender);
            }
        }
        send(new ViewReply(currentView), sender);
    }

    private void handleGetView(GetView m, Address sender) {
        // Your code here...
        send(new ViewReply(currentView), sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t) {
        // Your code here...
        Address primary = currentView.primary();
        Address backup = currentView.backup();
//        System.out.println(currentView);
        if (currentView.viewNum() != STARTUP_VIEWNUM && ack) {
//            System.out.println(serverAlive(primary) + " " + serverAlive(backup));
            if (!serverAlive(primary)) {
                if (serverAlive(backup)) {
                    Address newBackup = findNewBackup(backup);
                    updateView(backup, newBackup);
                }
            } else if (!serverAlive(backup)) {
                Address newBackup = findNewBackup(primary);
                if (backup != null || newBackup != null) {
                    updateView(primary, newBackup);
                }
            }
            aliveServers.clear();
        }
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private int nextViewNum() {
        return currentView.viewNum() + 1;
    }

    private void updateView(Address primary, Address backup) {
        currentView = new View(nextViewNum(), primary, backup);
        tentativeView = null;
        ack = false;
    }

    private boolean serverAlive(Address server) {
        return aliveServers.contains(server);
    }

    private Address findNewBackup(Address primary) {
        for (Address server : aliveServers) {
            if (!Objects.equals(server, primary)) {
                return server;
            }
        }
        return null;
    }
}
