package cpen431.mp.PoPRequestReply;

/**
 * Created by simon on 17.03.17.
 */
public enum PoPErrorCode {
    SUCCESS,
    PID_NOT_RUNNING,
    WRONG_PORT,
    ERROR_COMMAND,
    SERVER_TIMEOUT,
    INTERNAL_ERROR,
    SSH_ERROR,
    NO_JAVA_FILE,
    ERROR_UNRECOGNIZED_COMMAND,
    ERROR_UNKNOWN,
    NO_MEMORY_LIMIT;
}
