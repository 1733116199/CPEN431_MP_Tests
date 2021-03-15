package cpen431.mp.Utilities;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ServerNode {
	 private String hostName;
	 private int portNumber;
	 private int processID;
	 
	 public ServerNode(String hostName, int portNumber) {
		 this.hostName = hostName;
		 this.portNumber = portNumber;
	 }
	 
	 public String getHostName() {
		return this.hostName; 
	 }
	 
	 public int getPortNumber() {
		 return this.portNumber;
	 }
	 
	 public void setProcessID(int processID) {
		 this.processID = processID;
	 }
	 
	 public int getProcessID() {
		 return this.processID;
	 }

	/**
	   * Builds the server node list from the given server list file
	   * @param fileName
	   * @return the server node list
	   */
	  public static ArrayList<ServerNode> buildServerNodeList(String fileName) {
		  ArrayList<ServerNode> serverNodes = new ArrayList<>();
		  FileInputStream fin = null;
		  BufferedReader br = null;
		  try {
			  fin = new FileInputStream(fileName);
			  br = new BufferedReader(new InputStreamReader(fin));
			  String line = "";
			  while ((line = br.readLine()) != null) {
				String[] tokens = line.split(":");
                // this is too expensive
		    	/*try {
		    		 if (InetAddress.getByName(tokens[0]).isReachable(5000)) 
		    		 {
		    			 serverNodes.add(new ServerNode(tokens[0], Integer.parseInt(tokens[1])));
		    		 } 
		    	} catch (IOException e) {
		    		 e.printStackTrace();
		    	}*/		    	
				try {
					InetAddress.getByName(tokens[0]);
					serverNodes.add(new ServerNode(tokens[0], Integer.parseInt(tokens[1])));
				} catch (UnknownHostException e) {
					//e.printStackTrace();
					System.err.println("\nFailed to reach host: " + tokens[0]);
					System.err.println("Excluding from the server node list ... ");
				}			
			  }
		  } catch (IOException e) {
				System.err.println("Unable to parse the server list file!");
			} finally {
				try {
					fin.close();
					br.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		  return serverNodes;
	  }


	public static boolean ServersAreUnique(ArrayList<ServerNode> usedServers){
		Set<String> alreadyUsed = new HashSet<>();
		for(ServerNode nodeToCheck : usedServers){

			if(alreadyUsed.contains(nodeToCheck.getHostName())){
				System.err.println("Duplicated node : " + nodeToCheck.getHostName());
				return false;
			}
			alreadyUsed.add(nodeToCheck.getHostName());
		}
		return true;
	}
}