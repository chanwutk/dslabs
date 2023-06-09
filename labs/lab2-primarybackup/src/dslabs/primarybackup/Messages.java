package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Message;
import java.util.List;
import lombok.Data;

/* -------------------------------------------------------------------------
    ViewServer Messages
   -----------------------------------------------------------------------*/
@Data
class Ping implements Message {
    private final int viewNum;
}

@Data
class GetView implements Message {
}

@Data
class ViewReply implements Message {
    private final View view;
}

/* -------------------------------------------------------------------------
    Primary-Backup Messages
   -----------------------------------------------------------------------*/
@Data
class Request implements Message {
    // Your code here...
    private final AMOCommand amoCommand;
}

@Data
class Reply implements Message {
    // Your code here...
    private final AMOResult amoResult;
}

// Your code here...
@Data
class FRequest implements Message {
    private final AMOCommand amoCommand;
    private final Address sender;
}

@Data
class FReply implements Message {
    private final boolean accept;
    private final AMOCommand amoCommand;
    private final Address sender;
}

@Data
class STRequest implements Message {
    private final AMOApplication<Application> amoApplication;
    private final View view;
}

@Data
class STReply implements Message {
    private final boolean accept;
    private final View view;
}
