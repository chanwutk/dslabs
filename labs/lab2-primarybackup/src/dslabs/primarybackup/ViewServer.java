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
        aliveServers = new HashSet<>();
        ack = false;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // Your code here...
        aliveServers.add(sender);

        if (same(sender, currentView.primary()) &&
                m.viewNum() == currentView.viewNum()) {
            ack = true;
        }

        if (currentView.viewNum() == STARTUP_VIEWNUM) {
            currentView = new View(INITIAL_VIEWNUM, sender, null);
        }

        Address primary = currentView.primary();
        if (currentView.backup() == null) {
            Address backup = findNewBackup(primary);
            if (backup != null) {
                updateView(primary, backup);
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
        if (currentView.viewNum() != STARTUP_VIEWNUM && ack) {
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
        }
        aliveServers.clear();
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private boolean same(Address address1, Address address2) {
        return Objects.equals(address1, address2);
    }

    private int nextViewNum() {
        return currentView.viewNum() + 1;
    }

    private void updateView(Address primary, Address backup) {
        if (ack) {
            currentView = new View(nextViewNum(), primary, backup);
            ack = false;
        }
    }

    private boolean serverAlive(Address server) {
        return aliveServers.contains(server);
    }

    private Address findNewBackup(Address primary) {
        for (Address server : aliveServers) {
            if (!same(server, primary)) {
                return server;
            }
        }
        return null;
    }
}
