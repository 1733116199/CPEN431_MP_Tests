package cpen431.mp.KeyValue;

/**
 * WRONG_VALUE - PUT Value != GET Value
 * @author NOBODY
 *
 */
public enum KVSResponseStatus {
	SUCCESS, TIMEOUT, ERROR_CHECKSUM, ERROR_UID, ERROR_KVS, WRONG_VALUE, ERROR_INVALID_FORMAT
}
