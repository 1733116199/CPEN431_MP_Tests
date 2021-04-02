package cpen431.mp.PoPRequestReply;
/**
 * Created by simon on 17.03.17.
 */
public class PoPPRequest {
    public PoPRequestType type;
    public int port;
    public String hostname;
    public String secretCode;
    public String newCommand;
    public int pid;

    public String toString() {
        return type + " Request with secret code :" + secretCode+" , hostname: "+hostname;
    }
}
