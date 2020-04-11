package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import dslabs.primarybackup.View;
import java.util.HashSet;
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
    private View currentView = new View(STARTUP_VIEWNUM, null, null);
    private Set<Address> aliveServers = new HashSet<Address>();
    private Set<Address> prevAliveServers = new HashSet<Address>();

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
        // currentView = new View(STARTUP_VIEWNUM, null, null);
        // aliveServers = new HashSet<Address>();
        // prevAliveServers = new HashSet<Address>();
        // Address primary = currentView.primary();
        // Address backup = currentView.backup();
        // for (Address server : aliveServers) {
        //     if (primary == null) {
        //         primary = server;
        //     } else if (backup == null) {
        //         backup = server;
        //     } else {
        //         break;
        //     }
        // }
        // currentView = new View(INITIAL_VIEWNUM, primary, backup);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // Your code here...
        System.out.println(sender);
        aliveServers.add(sender);
        if (currentView.viewNum() == STARTUP_VIEWNUM) {
            currentView = new View(INITIAL_VIEWNUM, sender, null);
        } else if (currentView.backup() == null && sender.compareTo(currentView.primary()) != 0) {
            currentView = new View(currentView.viewNum() + 1, currentView.primary(), sender);
        }
        send(new ViewReply(currentView), sender);
    }

    private void handleGetView(GetView m, Address sender) {
        // Your code here...
        System.out.println(currentView);
        send(new ViewReply(currentView), sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t) {
        // Your code here...
        Address primary = currentView.primary();
        Address backup = currentView.backup();
        if (!serverAlive(primary)) {
            if (!serverAlive(backup)) {
                // TODO: dies?
                throw new IllegalArgumentException();
            } else {
                replaceBackup(backup);
            }
        } else if (!serverAlive(backup)) {
            replaceBackup(primary);
        }
        prevAliveServers = aliveServers;
        aliveServers = new HashSet<Address>();
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private boolean serverAlive(Address server) {
        return aliveServers.contains(server) || prevAliveServers.contains(server);
    }

    private void replaceBackup(Address newPrimary) {
        Address newBackup = findNewBackup(newPrimary);
        if (newBackup.compareTo(currentView.backup()) != 0 ||
                newPrimary.compareTo(currentView.primary()) != 0) {
            // TODO: copy k-v to new backup.
            currentView = new View(currentView.viewNum() + 1, newPrimary, newBackup);
        }
    }

    private Address findNewBackup(Address primary) {
        for (Address server : aliveServers) {
            if (server.compareTo(primary) != 0) {
                return server;
            }
        }

        for (Address server : prevAliveServers) {
            if (server.compareTo(primary) != 0) {
                return server;
            }
        }
        return null;
    }
}
