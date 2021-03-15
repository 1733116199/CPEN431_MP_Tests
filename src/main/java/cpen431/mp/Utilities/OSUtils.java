package cpen431.mp.Utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;

public abstract class OSUtils {
	public static final String OS_TYPE = System.getProperty("os.name").toLowerCase();
	public static final String OS_ERROR = "ERROR";
	
	public static String executeCommand(String commandInputString) {
		//TODO: Return all the lines and parse within the calling method.
	    String commandOutputString = "";
	    String line = "";
	    Process process = null;
	    ArrayList<String> commandOutput = null;
	    BufferedReader br = null;
	
	    try {
	      commandOutput = new ArrayList<String>();
	      process = Runtime.getRuntime().exec(commandInputString);
	      br = new BufferedReader(new InputStreamReader(process.getInputStream()));
	      while ((line = br.readLine()) != null) {
	        //System.out.println("$ " + line);
	        commandOutput.add(line);
	      }
	      process.waitFor();
	      //System.out.println("$ " + process.exitValue());
	      process.destroy();
	      commandOutputString = commandOutput.get(0);
	    } catch (Exception e) {
	      commandOutputString = OS_ERROR;
	      System.err.println("Error executing native Linux command.");
	    } finally {
	      try {
	        br.close();
	      } catch (IOException ex) {
	        ex.printStackTrace();
	      }
	    }
	    return commandOutputString;
	}
	
	public static String loadavg() {
		String outputString = "";
		if (OS_TYPE.indexOf("nux") >= 0) {
	      //System.out.println("Linux System.");
	      String line = executeCommand("cat /proc/loadavg");
	      if (!line.equalsIgnoreCase(OS_ERROR)) {
	        //System.out.println("$ " + line);		        
	        System.out.println("[ CPU load (loadavg, last one minute) " + line.split(" ")[0] + " ]");
	        outputString = line.split(" ")[0];
	      } else {
	        System.err.println("Error executing native Linux command.");
	        outputString = "Error executing native Linux command.";
	      }
	    } else {
	      //System.out.println("Not a Linux System.");
	      outputString = "Not a Linux System."; 	
	    }
	    return outputString;
	}
	
	public static String getPIDOfJavaExecutable() {
		return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	}
}
