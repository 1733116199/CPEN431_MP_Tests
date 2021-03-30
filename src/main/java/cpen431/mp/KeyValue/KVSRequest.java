package cpen431.mp.KeyValue;

import cpen431.mp.Utilities.ServerNode;

import java.util.ArrayList;
import java.util.Arrays;

public class KVSRequest {
	public ArrayList<ServerNode> serverNodes;
	public KVSRequestType type;
	public byte[] requestUID;
	public byte[] key;
	public byte[] value;
	public int version;

	public String toString() {
		return type + " Request #" + requestUID + ": (" + Arrays.toString(key) +
				"," + Arrays.toString(value) + ")";
	}
}
