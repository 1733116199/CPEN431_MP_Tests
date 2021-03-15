package cpen431.mp.Statistics;

import cpen431.mp.KeyValue.KVSErrorCode;
import cpen431.mp.KeyValue.KVSRequest;
import cpen431.mp.KeyValue.KVSRequestType;
import cpen431.mp.KeyValue.KVSResponse;
import cpen431.mp.KeyValue.KVSResponseStatus;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Client activity logging for the lifetime of the application or a on a per test case basis.
 * @author NOBODY
 *
 */
public class KVSClientLog {
	public class MessageExchange {
		private long timeStamp; 
		private String uniqueID;
		private KVSRequestType command;
		private String key;
		private String requestValue;
		private String responseValue;
		private KVSRequest request;
		private KVSResponse response;
		private KVSErrorCode errorCode;
		private KVSResponseStatus responseStatus;
		
		// TODO: set hex string of uniqueID, key, value 
		
		public MessageExchange(long timeStamp) {
			this.timeStamp = timeStamp;
		}
		
		public MessageExchange(long timeStamp, String uniqueID, KVSRequest request, KVSResponse response) {
			this.timeStamp = timeStamp;
			this.uniqueID = uniqueID;
			this.request = request;
			this.response = response; 
		}
		
		// for all requests
		public MessageExchange(long timeStamp, String uniqueID, KVSRequestType command, String key, String requestValue, String responseValue, KVSErrorCode errorCode, KVSResponseStatus responseStatus) {
			this.timeStamp = timeStamp;
			this.uniqueID = uniqueID;
			this.command = command;
			this.key = key;
			this.requestValue = requestValue;
			this.responseValue = responseValue; 
			this.errorCode = errorCode;
			this.responseStatus = responseStatus; 
		}		
		
		// for failed responses, ignores values
		public MessageExchange(long timeStamp, String uniqueID, KVSRequestType command, String key, KVSErrorCode errorCode, KVSResponseStatus responseStatus) {
			this.timeStamp = timeStamp;
			this.uniqueID = uniqueID;
			this.command = command;
			this.key = key;
			this.errorCode = errorCode;
			this.responseStatus = responseStatus; 
		}
		
		// for out-of-order responses
		public MessageExchange(long timeStamp, String uniqueID) {
			this.timeStamp = timeStamp;
			this.uniqueID = uniqueID;
		}
				
		public long getTiemStamp() {
			return this.timeStamp;
		}
		
		public String getUniqueID() {
			return this.uniqueID;
		}
		
		public KVSRequestType getCommand() {
			return this.command;
		}
		
		public String getKey() {
			return this.key;
		}
		
		public String getRequestValue() {
			return this.requestValue;
		}
		
		public String getResponseValue() {
			return this.responseValue;
		}
		
		public KVSRequest getKVSRequest() {
			return this.request;
		}
		
		public KVSResponse getKVSResponse() {
			return this.response;
		}
		
		public KVSErrorCode getKVSErrorCode() {
			return this.errorCode;
		}
		
		public KVSResponseStatus getKVSResponseStatus() {
			return this.responseStatus;
		}
	}
	private int clintID;
	
    public ArrayList<MessageExchange> messageExchanges;
    public ArrayList<MessageExchange> messageExchangesError;
    public ArrayList<MessageExchange> messageExchangesOutOfOrder;
    
    public static KVSClientLog[] kvsClientLogs; // must be initialized before log collection
    
    public static boolean enableLogging = false;
    public static boolean logAll = false;
    public static boolean logError = false;
    public static boolean logOutOfOrder = false;
    public static boolean logErrorCapacityTest = false;
    
    public KVSClientLog(int clientID) {
    	this.clintID = clientID;
    	this.messageExchanges = new ArrayList<MessageExchange>();
    	this.messageExchangesError = new ArrayList<MessageExchange>();
    	this.messageExchangesOutOfOrder = new ArrayList<MessageExchange>();    	
    }
		
	public int getClintID() {
		return this.clintID;
	}
	
	// all the logs (for a type) for a test go to one file
	public static void writeClientLogsToFile(String fileName) {
		// create the log file(s)
		
		for (KVSClientLog cl : kvsClientLogs) {
			
			if (logAll) {
				// all
				for (int i = 0; i < cl.messageExchanges.size(); i++) {
					MessageExchange me = cl.messageExchanges.get(i);
					String outputString = cl.getClintID() + "," + 
							me.getTiemStamp() + "," +
							me.getUniqueID() + "," +
							me.getCommand() + "," +
							me.getKey() + "," +
							me.getRequestValue() + "," +
							me.getResponseValue() + "," +
							me.getKVSErrorCode() + "," +
							me.getKVSResponseStatus() + "";
					// append output string to file
					appendLineToFile(fileName + "_all", outputString);
				}
			}	
			
			if (logError) {
				// error
				for (int i = 0; i < cl.messageExchangesError.size(); i++) {
					MessageExchange me = cl.messageExchangesError.get(i);
					String outputString = cl.getClintID() + "," + 
							me.getTiemStamp() + "," +
							me.getUniqueID() + "," +
							me.getCommand() + "," +
							me.getKey() + "," +
							me.getRequestValue() + "," +
							me.getResponseValue() + "," +
							me.getKVSErrorCode() + "," +
							me.getKVSResponseStatus() + "";
					// append output string to file
					appendLineToFile(fileName + "_error", outputString);
				}
			}
			
			if (logOutOfOrder) {
				// out-of-order messages
				for (int i = 0; i < cl.messageExchangesOutOfOrder.size(); i++) {
					MessageExchange me = cl.messageExchangesOutOfOrder.get(i);
					String outputString = cl.getClintID() + "," + 
							me.getTiemStamp() + "," +
							me.getUniqueID() + "";
					// append output string to file
					appendLineToFile(fileName + "_out_of_order", outputString);
				}
			}
			
			if (logErrorCapacityTest) {
				// error logs only for the capacity test
				for (int i = 0; i < cl.messageExchangesError.size(); i++) {
					MessageExchange me = cl.messageExchangesError.get(i);
					String outputString = cl.getClintID() + "," + 
							me.getTiemStamp() + "," +
							me.getUniqueID() + "," +
							me.getCommand() + "," +
							me.getKey() + "," +							
							me.getKVSErrorCode() + "," +
							me.getKVSResponseStatus() + "";
					// append output string to file
					appendLineToFile(fileName + "_error", outputString);
				}
			}
		}
	}
	
	public static void appendLineToFile(String fileName, String line) {
		//System.out.println(line);
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(fileName, true));
			bw.write(line);	
			bw.newLine();
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void enableClientLogging(boolean a, boolean e, boolean o, boolean c) {		
		KVSClientLog.logAll = a;
		KVSClientLog.logError = e;
		KVSClientLog.logOutOfOrder = o;
		KVSClientLog.logErrorCapacityTest = c;
		KVSClientLog.enableLogging = a | e | o | c;
	}
	
	public static void disableClientLogging() {
		KVSClientLog.enableLogging = false;
		KVSClientLog.logAll = false;
		KVSClientLog.logError = false;
		KVSClientLog.logOutOfOrder = false;
		KVSClientLog.logErrorCapacityTest = false;
	}
	
	public static void initializeClinetLogs(int numberOfClients) {
		if(KVSClientLog.enableLogging) {				
			KVSClientLog.kvsClientLogs = new KVSClientLog[numberOfClients]; 
			for (int i = 0; i < KVSClientLog.kvsClientLogs.length; i++) {
				KVSClientLog.kvsClientLogs[i] = new KVSClientLog(i); // create log object for each client 
			}
		}
	}
}
 