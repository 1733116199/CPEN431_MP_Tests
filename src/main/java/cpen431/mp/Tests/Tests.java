package cpen431.mp.Tests;

import java.util.Random;

public abstract class Tests {
	public static long testStartTime = 0;
	
	static public void testUtilStartTimer() {
		testStartTime = System.currentTimeMillis();
	}
	
	static public void testUtilElapsedTime() {
		double testElapsedTimeSeconds = (double)(System.currentTimeMillis() - testStartTime)/1000.0;
		printMessage("... TEST Completed in " + testElapsedTimeSeconds + " seconds");
	}

	static void pause(long seconds) {
		try {
	        Thread.sleep(seconds * 1000);
	    } catch (InterruptedException e) {
	    	e.printStackTrace();
	    }
	}

	public static String toHumanReadableString(String string) {
		if (string.equalsIgnoreCase("6E756C6C")) {
			return "null";
		} else return string;
	}
				
	static void printMessage(String str) {
		System.out.println("[ "+ str + " ]");
	}

	public static int getRandomInteger(int min, int max, Random rand) {
		return rand.nextInt((max - min) + 1) + min;
	}
}
