package cpen431.mp.Utilities;

import cpen431.mp.PoPRequestReply.PoPClientAPI;
import cpen431.mp.PoPRequestReply.PoPErrorCode;
import cpen431.mp.PoPRequestReply.PoPPReply;
import cpen431.mp.Tests.TestsConfig;
import java.util.ArrayList;
import java.util.HashMap;

public class PoPUtils {

    public static String checkSumForSingleJar = "MD5 Jar";
    public static String checkSumForAllFiles = "MD5 All";

    public static boolean isSuccess(PoPPReply reply, boolean strictCheck){
        if (strictCheck) {
            if (reply.errorCode != PoPErrorCode.SUCCESS)
                return false;
        }else {
            if (reply.errorCode == PoPErrorCode.NO_MEMORY_LIMIT ||
                    reply.errorCode == PoPErrorCode.PID_NOT_RUNNING ||
                    reply.errorCode == PoPErrorCode.WRONG_PORT ||
                    reply.errorCode == PoPErrorCode.SSH_ERROR ||
                    reply.errorCode == PoPErrorCode.SERVER_TIMEOUT)
                return false;
        }
        return true;
    }

    public static PoPPReply killServerProcess(String node, String secretCode, int pid) {
        Boolean killed = false;
        PoPPReply reply = PoPClientAPI.kill(node, secretCode, pid);
        return reply;
    }

    public static boolean getCodeMD5(String secretKey, ArrayList<ServerNode> nodes, HashMap<String, String> checkSum) {

        try {
            int index = -1;
            for (int i = 0; i < nodes.size(); i++) {
                PoPPReply reply = PoPClientAPI.getMD5All(nodes.get(i).getHostName(), secretKey, nodes.get(i).getProcessID());
                if (!isSuccess(reply, true)) {
                    System.err.println("[Error] Failed to connect to PoP Server for MD5 checking. Aborting!");
                    return false;
                }
                if (isSuccess(reply, true)) {
                    checkSum.put(checkSumForAllFiles, reply.jarMD5);
                    index = i;
                    break;
                }
            }
            int successNodes = 0;
            String checkSums = "";
            // Loop on the remaining nodes to get the Jars MD5
            for (int i = 0; i < nodes.size(); i++) {
                if (successNodes >= TestsConfig.getPopNodesThreshold()) {
                    break;
                } else {
                    if (i != index) {
                        PoPPReply reply = PoPClientAPI.getMD5Jar(nodes.get(i).getHostName(), secretKey, nodes.get(i).getProcessID());
                        if (isSuccess(reply, true)) {
                            successNodes++;
                            checkSums += reply.jarMD5 + " ";
                        }
                    }
                }
            }
            checkSum.put(checkSumForSingleJar, checkSums);
        } catch (Exception e){
            System.err.println("[Error] Failed to compute checksum for source code and application Jar!");
            return false;
        }
        return true;
    }

    public static boolean suspendNode(String node, String secretKey, int processID){
        boolean suspended = false;
        try{
            System.out.println("Suspend command sent.");
            PoPPReply reply = PoPClientAPI.suspendPid(node, secretKey, processID);
            if (reply.errorCode == PoPErrorCode.SUCCESS){
                suspended = true;
            }
        } catch (Exception e ) {
            System.out.println("[Warning] Failed to suspend node " + node);
        }
        return suspended;
    }

    public static void resumeNodes(ArrayList<ServerNode> nodes, String secretKey) {
        for (ServerNode node : nodes) {
            try {
                PoPPReply reply = PoPClientAPI.resumePid(node.getHostName(), secretKey, node.getProcessID());
                if (reply.errorCode != PoPErrorCode.SUCCESS){
                    throw new Exception("Resume failed. Error code: " + reply.errorCode);
                }
            } catch (Exception e) {
                System.out.println("[Warning] Failed to resume node " + node.getHostName());
            }

        }
    }
}
