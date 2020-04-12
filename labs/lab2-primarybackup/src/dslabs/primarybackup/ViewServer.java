package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
    private View newView;
    private Set<Address> aliveServers;
    private Map<Address, Integer> serversView;
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
        serversView = new HashMap<>();
        ack = false;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // Your code here...
        System.out.println(sender + " " + m.viewNum() + " " + currentView.viewNum());
        aliveServers.add(sender);
        if (!serversView.containsKey(sender) || serversView.get(sender) < m.viewNum()) {
            serversView.put(sender, m.viewNum());
        }

        if (Objects.equals(sender, currentView.primary()) && m.viewNum() == currentView.viewNum()) {
            if (newView != null) {
                currentView = newView;
                newView = null;
            }
        }

        Address primary = currentView.primary();
        int currentViewNum = currentView.viewNum();
        if (currentViewNum == STARTUP_VIEWNUM) {
            currentView = new View(INITIAL_VIEWNUM, sender, null);
            ack = false;
        } else if (currentView.backup() == null && !Objects.equals(sender, primary)) {
            if (serversView.get(currentView.primary()) == currentView.viewNum()) {
                currentView = new View(currentViewNum + 1, primary, sender);
                newView = null;
            } else {
                newView = new View(currentViewNum + 1, primary, sender);
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
        int viewNum = currentView.viewNum();
        System.out.println(currentView);
        if (viewNum != STARTUP_VIEWNUM && serversView.get(primary) == viewNum) {
            System.out.println(serverAlive(primary) + " " + serverAlive(backup));
            if (!serverAlive(primary)) {
                if (!serverAlive(backup)) {
                    // TODO: dies?
                    throw new IllegalArgumentException();
                } else {
                    Address newBackup = findNewBackup(backup);
                    if (serversView.get(primary) == viewNum) {
                        currentView = new View(currentView.viewNum() + 1, backup, newBackup);
                        newView = null;
                    } else {
                        newView = new View(currentView.viewNum() + 1, backup, newBackup);
                    }
                }
            } else if (!serverAlive(backup)) {
                Address newBackup = findNewBackup(primary);
                if (backup != null || newBackup != null) {
                    if (serversView.get(primary) == viewNum) {
                        currentView = new View(currentView.viewNum() + 1, primary, newBackup);
                        newView = null;
                    } else {
                        newView = new View(currentView.viewNum() + 1, primary, newBackup);
                    }
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

    private boolean serverAlive(Address server) {
        return aliveServers.contains(server);
//        return serversView.containsKey(server);
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
