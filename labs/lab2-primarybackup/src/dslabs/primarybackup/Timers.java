package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
    static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
    static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    private final AMOCommand command;
}

// Your code here...
@Data
final class ForwardingRequestTimer implements Timer {
    static final int FORWARDING_REQUEST_MILLIS = 50;

    private final ForwardingRequest forwardingRequest;
    private final Address backup;
}

@Data
final class StateTransferTimer implements Timer {
    static final int STATE_TRANSFER_MILLIS = 50;

    private final Address backup;
}

@Data
final class ClientGetViewTimer implements Timer {
    static final  int GET_VIEW_MILLIS = 50;
}