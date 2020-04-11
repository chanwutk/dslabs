package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import dslabs.primarybackup.View;
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
    private Set<Address> prevAliveServers;
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
        aliveServers = new HashSet<Address>();
        prevAliveServers = new HashSet<Address>();
        ack = false;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // Your code here...
        System.out.println(sender);
        aliveServers.add(sender);

        Address primary = currentView.primary();
        int currentViewNum = currentView.viewNum();

        ack = Objects.equals(primary, sender) && m.viewNum() == currentViewNum;

        if (currentViewNum == STARTUP_VIEWNUM) {
            currentView = new View(INITIAL_VIEWNUM, sender, null);
        } else if (currentView.backup() == null && !Objects.equals(sender, primary) && ack) {
            currentView = new View(currentViewNum + 1, primary, sender);
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
        int viewNum = currentView.viewNum();
        System.out.println(currentView);
        if (viewNum != STARTUP_VIEWNUM && ack) {
            System.out.println(serverAlive(primary) + " " + serverAlive(backup));
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
        }
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private boolean serverAlive(Address server) {
        return aliveServers.contains(server); // || prevAliveServers.contains(server);
    }

    private void replaceBackup(Address newPrimary) {
        Address newBackup = findNewBackup(newPrimary);
        if (!Objects.equals(newBackup, currentView.backup()) ||
                !Objects.equals(newPrimary, currentView.primary())) {
            // TODO: copy k-v to new backup.
            if (!Objects.equals(newPrimary, currentView.primary())) {
                ack = false;
            }
            currentView = new View(currentView.viewNum() + 1, newPrimary, newBackup);
        }
    }

    private Address findNewBackup(Address primary) {
        for (Address server : aliveServers) {
            if (!Objects.equals(server, primary)) {
                return server;
            }
        }

        // for (Address server : prevAliveServers) {
        //     if (server.compareTo(primary) != 0) {
        //         return server;
        //     }
        // }

        return null;
    }
}
