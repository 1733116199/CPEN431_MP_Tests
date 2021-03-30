package cpen431.mp.PoPRequestReply;

/**
 * Created by simon on 17.03.17.
 */
public class PoPPReply {
    public PoPResponseStatus status;
    public PoPRequestType requestType;
    public PoPErrorCode errorCode;
    public int exitCode;
    public String commandOutput;
    public String jarMD5;

    public String toString() {
        return requestType + " Response: " + status + ", error code :" + errorCode + ", " +
                "exitCode : " + exitCode + ", commandOutput : " + commandOutput + ", jarMD5 : "+jarMD5;

    }
}
