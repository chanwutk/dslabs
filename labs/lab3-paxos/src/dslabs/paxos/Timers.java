package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Timer;
import lombok.Data;
import lombok.NonNull;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    @NonNull
    private final PaxosRequest paxosRequest;
}

// Your code here...
@Data
final class HeartbeatCheckTimer implements Timer {
    static final int PING_CHECK_TIMER = 100;
}
