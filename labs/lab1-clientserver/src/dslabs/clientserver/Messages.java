package dslabs.clientserver;

import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Result;
import lombok.Data;

@Data
class Request implements Message {
    // Your code here...
    private final Command command;
    private final int sequenceNum;
}

@Data
class Reply implements Message {
    // Your code here...
    private final Result result;
    private final int sequenceNum;
}
