package cpen431.mp.KeyValue;

import java.util.Arrays;

public class KVSResponse {
	public KVSResponseStatus status;
	public KVSRequestType requestType;
	public KVSErrorCode errorCode;
	public byte[] requestUID;
	public long responseTime;
	public int retries;
	public byte[] key;
	public byte[] value;
	public int pid;
	public boolean validStat = true;
	public int version;

	public String toString() {
		return requestType + " Response #" + requestUID + ": " + status + ", " + errorCode + ", " +
				responseTime + "ms, " + retries + "retries, (" + Arrays.toString(key) + "," +
				Arrays.toString(value) + "," + validStat + ")";
	}
}
