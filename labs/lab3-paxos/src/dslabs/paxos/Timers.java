package dslabs.paxos;

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
    static final int HB_CHECK_TIMER = 100;
}

@Data
final class HeartbeatTimer implements Timer {
    static final int HB_TIMER = 25;

    @NonNull private final BallotNum leader_ballot;
}

@Data
final class P2aTimer implements Timer {
    static final int P2A_TIMER = 25;

    @NonNull private final P2aMessage p2a;
}