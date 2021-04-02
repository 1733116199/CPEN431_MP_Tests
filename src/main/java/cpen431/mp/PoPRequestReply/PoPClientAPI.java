package cpen431.mp.PoPRequestReply;


import cpen431.mp.Network.TCPClient;
import cpen431.mp.ProtocolBuffers.PoPRequest.PRequest;
import cpen431.mp.ProtocolBuffers.PoPReply.PReply;

import java.io.IOException;

import static cpen431.mp.PoPRequestReply.PoPErrorCode.*;

/**
 * Created by Simon on 17.03.17.
 */
public class PoPClientAPI {
    public static PoPPReply comparePID(String hostname, String secretCode, int pid, int port) {
        PoPPRequest request = new PoPPRequest();
        request.type = PoPRequestType.COMPARE_PID;
        request.hostname=hostname;
        request.secretCode = secretCode;
        request.pid = pid;
        request.port = port;
        return sendRequest(request);
    }
    public static PoPPReply execNewCommand(String hostname, String secretCode, String command){
        PoPPRequest request = new PoPPRequest();
        request.type = PoPRequestType.NEW_CMD;
        request.hostname=hostname;
        request.secretCode = secretCode;
        request.newCommand = command;
        return sendRequest(request);
    }
    public static PoPPReply resumePid(String hostname, String secretCode, int pid){
        PoPPRequest request = new PoPPRequest();
        request.type = PoPRequestType.RESUME_PID;
        request.hostname=hostname;
        request.secretCode = secretCode;
        request.pid = pid;
        return sendRequest(request);
    }
    public static PoPPReply suspendPid(String hostname, String secretCode, int pid){
        PoPPRequest request = new PoPPRequest();
        request.type = PoPRequestType.SUSPEND_PID;
        request.hostname=hostname;
        request.secretCode = secretCode;
        request.pid =pid;
        return sendRequest(request);
    }
    public static PoPPReply kill(String hostname, String secretCode, int pid){
        PoPPRequest request = new PoPPRequest();
        request.type = PoPRequestType.KILL;
        request.hostname=hostname;
        request.secretCode = secretCode;
        request.pid = pid;
        return sendRequest(request);
    }
    public static PoPPReply getMD5All(String hostname, String secretCode, int pid){
        PoPPRequest request = new PoPPRequest();
        request.type = PoPRequestType.GET_MD5_ALL;
        request.hostname=hostname;
        request.secretCode = secretCode;
        request.pid = pid;
        return sendRequest(request);
    }

    public static PoPPReply getMD5Jar(String hostname, String secretCode, int pid){
        PoPPRequest request = new PoPPRequest();
        request.type = PoPRequestType.GET_MD5_JAR;
        request.hostname=hostname;
        request.secretCode = secretCode;
        request.pid = pid;
        return sendRequest(request);
    }

    public static PoPPReply sendRequest(PoPPRequest request){
        byte[] payload = packPayload(request);
        byte[] receive = TCPClient.sendAndReceive(payload);
        PoPPReply response = unpackResponse(receive, request);
        return response;
    }

    public static byte[] packPayload(PoPPRequest request){
        byte[] payload = null;

        switch(request.type){
            case COMPARE_PID:
                payload = PRequest.newBuilder()
                        .setCommand(1)
                        .setHostname(request.hostname)
                        .setSecretCode(request.secretCode)
                        .setPid(request.pid)
                        .setPort(request.port)
                        .build().toByteArray();
            break;
            case KILL:
                payload = PRequest.newBuilder()
                        .setCommand(2)
                        .setHostname(request.hostname)
                        .setSecretCode(request.secretCode)
                        .setPid(request.pid)
                        .build().toByteArray();
            break;
            case GET_MD5_ALL:
                payload = PRequest.newBuilder()
                        .setCommand(3)
                        .setHostname(request.hostname)
                        .setSecretCode(request.secretCode)
                        .setPid(request.pid)
                        .build().toByteArray();
            break;
            case NEW_CMD:
                payload = PRequest.newBuilder()
                        .setCommand(4)
                        .setHostname(request.hostname)
                        .setSecretCode(request.secretCode)
                        .setNewCommand(request.newCommand)
                        .build().toByteArray();
            break;
            case SUSPEND_PID:
                payload = PRequest.newBuilder()
                        .setCommand(5)
                        .setHostname(request.hostname)
                        .setSecretCode(request.secretCode)
                        .setPid(request.pid)
                        .build().toByteArray();
            break;
            case RESUME_PID:
                payload = PRequest.newBuilder()
                        .setCommand(6)
                        .setHostname(request.hostname)
                        .setSecretCode(request.secretCode)
                        .setPid(request.pid)
                        .build().toByteArray();
            break;
            case GET_MD5_JAR:
                payload = PRequest.newBuilder()
                        .setCommand(7)
                        .setHostname(request.hostname)
                        .setSecretCode(request.secretCode)
                        .setPid(request.pid)
                        .build().toByteArray();
                break;
            default:
                System.err.println("Invalid request type !");

        }
        return payload;
    }
    public static PoPPReply unpackResponse(byte[] receive, PoPPRequest request){
        PoPPReply response = new PoPPReply();
        response.requestType = request.type;
        try{
            PReply reply= PReply.parseFrom(receive);
            switch(request.type){
                case COMPARE_PID:
                case KILL:
                case RESUME_PID:
                case SUSPEND_PID:
                    response.errorCode = translateErrorCode(reply.getErrCode());
                    response.status = PoPResponseStatus.SUCCESS;
                    break;
                case NEW_CMD:
                    response.errorCode = translateErrorCode(reply.getErrCode());
                    response.status = PoPResponseStatus.SUCCESS;
                    response.commandOutput = reply.getCommandOutput();
                    response.exitCode = reply.getExitCode();
                    break;
                case GET_MD5_ALL:
                case GET_MD5_JAR:
                    response.errorCode = translateErrorCode(reply.getErrCode());
                    response.status = PoPResponseStatus.SUCCESS;
                    response.jarMD5 = reply.getJarMD5();
                    break;
                default:
                    System.err.println("Invalid request type");
            }
        }catch(IOException e){
            System.err.println("Error passing raw data to ProtoBuffer class");
        }
        return response;
    }

    public static PoPErrorCode translateErrorCode(int errCode){
        switch(errCode){
            case 0:
                return PoPErrorCode.SUCCESS;

            case 1:
                return PID_NOT_RUNNING;
            case 2:
                return ERROR_COMMAND;
            case 3:
                return ERROR_UNRECOGNIZED_COMMAND;
            case 4:
                return SERVER_TIMEOUT;
            case 5:
                return SSH_ERROR;
            case 6:
                return INTERNAL_ERROR;
            case 7:
                return WRONG_PORT;
            case 8:
                return NO_JAVA_FILE;
            case 9:
                return NO_MEMORY_LIMIT;


            default:
                System.err.println("Unknown error code !");
        }
        return ERROR_UNKNOWN;
    }
}
