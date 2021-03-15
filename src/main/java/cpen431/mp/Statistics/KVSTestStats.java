package cpen431.mp.Statistics;

import cpen431.mp.KeyValue.KVSRequestType;
import cpen431.mp.KeyValue.KVSResponse;
import cpen431.mp.KeyValue.KVSResponseStatus;
import cpen431.mp.KeyValue.KVSResultField;
import cpen431.mp.Tests.TestStatus;

import java.util.ArrayList;

public class KVSTestStats {
	private KVSResponseStats responseStats = new KVSResponseStats();
	private TestStatus testStatus;
	private long testDuration;

	private String getPercentage(long value, long total) {
		if (total == 0) {
			return "NaN";
		}

		return Double.toString((value * 100) / (double)(total));
	}

	public long getResponseCount() {
		return responseStats.COUNT_TOTAL;
	}

	public int getClientCount() {
		return responseStats.getClientCount();
	}

	public TestStatus getTestStatus() {
		return testStatus;
	}

	public long getTestDuration() {
		return testDuration;
	}

	public void setTestStatus(TestStatus testStatus) {
		this.testStatus = testStatus;
	}

	public void setTestDuration(long testDuration) {
		this.testDuration = testDuration;
	}

	public void addResponses(int clientID, ArrayList<KVSResponse> responses) {
		this.responseStats.addKVSResponses(clientID, responses);
	}

	private void printCountStats(long total, long retries, long success, long timeout,
	                             long error, long error_kvs, long error_uid) {
		System.out.println(
				"Total Requests (without replies): " + total + String.format("%n") +
						"Total Requests (including replies): " + retries + String.format("%n") +
						"Successful Responses: " + success + String.format("%n") +
						"Timeout Responses: " + timeout +  String.format("%n") +
						"Total Failed Responses: " + error +  String.format("%n") +
//						"Failed (KVS) Responses: " + error_kvs + String.format("%n") +
//						"Failed (UID) Responses: " + error_uid +  String.format("%n") +
						"% Success: " + getPercentage(success, total) + String.format("%n") +
						"% Timeout: " + getPercentage(timeout, total) + String.format("%n") +
						"% Failed: " + getPercentage(error, total)
		);
	}

	private void printResponseTimeStats(long max, long min, double avg, double stdev) {
		System.out.println(
				"Response Time (Successful requests):" + String.format("%n") +
						"[max] " + max + "ms, [min] " + min + "ms" + String.format("%n") +
						"[avg] " + avg + " ms, [stdev] " + stdev + "ms"
		);
	}

	public void printAggregateTypeSummaryStats(KVSRequestType type) {
		long total = responseStats.getCount(null, type, -1);
		long retries = responseStats.getRetrySum(null, type, -1);
		long success = responseStats.getCount(KVSResponseStatus.SUCCESS, type, -1);
		long timeout = responseStats.getCount(KVSResponseStatus.TIMEOUT, type, -1);
		long error_kvs = responseStats.getCount(KVSResponseStatus.ERROR_KVS, type, -1);
		long error_uid = responseStats.getCount(KVSResponseStatus.ERROR_UID, type, -1);
		long error = error_kvs + error_uid;

		long max_rt = responseStats.getMaxResponseTime(KVSResponseStatus.SUCCESS, type, -1);
		long min_rt = responseStats.getMinResponseTime(KVSResponseStatus.SUCCESS, type, -1);
		double avg_rt = responseStats.getAvgResponseTime(KVSResponseStatus.SUCCESS, type, -1);
		double stdev_rt = responseStats.getStdevResponseTime(KVSResponseStatus.SUCCESS, type, -1);

		if (total != 0) {
			System.out.println("Command: " + type);
			printCountStats(total, retries, success, timeout, error, error_kvs, error_uid);
			printResponseTimeStats(max_rt, min_rt, avg_rt, stdev_rt);
			System.out.println("Throughput: " + (double) 1000 * total / testDuration + " requests per second");
			System.out.println("Goodput: " + (double) 1000 * success / testDuration + " requests per second");
			System.out.println("Retry Rate (Successful requests): " + (double) retries / total);
		}
		else {
			System.out.println("Command " + type + " not observed during test!");
		}

	}

	public void printAggregateSummaryStats(ArrayList<KVSResultField> resultMap, String testName) {
		long total = responseStats.getCount(null, null, -1);
		long retries = responseStats.COUNT_RETRIES;
		long success = responseStats.getCount(KVSResponseStatus.SUCCESS, null, -1);
		long timeout = responseStats.getCount(KVSResponseStatus.TIMEOUT, null, -1);
		long error_kvs = responseStats.getCount(KVSResponseStatus.ERROR_KVS, null, -1);
		long error_uid = responseStats.getCount(KVSResponseStatus.ERROR_UID, null, -1);
		long error = error_kvs + error_uid;

		long max_rt = responseStats.getMaxResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		long min_rt = responseStats.getMinResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		double avg_rt = responseStats.getAvgResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		double stdev_rt = responseStats.getStdevResponseTime(KVSResponseStatus.SUCCESS, null, -1);

		int clients = responseStats.getClientCount();

		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Goodput (Requests per Second)",
				Math.ceil((double) 1000 * success / testDuration)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Maximum Response Time (ms)",
				Math.ceil(max_rt)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Average Response Time (ms)",
				Math.ceil(avg_rt)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) STDEV Response Time (ms)",
				Math.ceil(stdev_rt)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Total Requests (without replies)",
				Long.toString(total)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Total Requests (including replies)",
				Long.toString(retries)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Successful Responses",
				Long.toString(success)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Timeout Responses",
				Long.toString(timeout)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Total Failed Responses",
				Long.toString(error)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) % Success",
				getPercentage(success, total)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) % Timeout",
				getPercentage(timeout, total)));
		resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) % Failed",
				getPercentage(error, total)));

		System.out.println(
//				"---------------------------------------" + String.format("%n") +
				testName + ": Test Status: " + testStatus + String.format("%n") +
						"Clients: " + responseStats.getClientCount()
		);

		if (total != 0) {
			printCountStats(total, retries, success, timeout, error, error_kvs, error_uid);
			printResponseTimeStats(max_rt, min_rt, avg_rt, stdev_rt);
			System.out.println("Throughput: " + (double) 1000 * total / testDuration + " requests per second");
			System.out.println("Goodput: " + (double) 1000 * success / testDuration + " requests per second");
			System.out.println("Retry Rate (Successful requests): " + (double) retries / total);
		}
		else {
			System.out.println("No packets observed during test!");
		}
	}
	
	public void printAggregateSummaryStats(ArrayList<KVSResultField> resultMap, String testName, String cpuLoadAvg) {
		printAggregateSummaryStats(resultMap, testName, cpuLoadAvg, 0);
	}
	
	public void printAggregateSummaryStats(ArrayList<KVSResultField> resultMap, String testName, String cpuLoadAvg, int numberOfClients) {
		long total = responseStats.getCount(null, null, -1);
		long retries = responseStats.COUNT_RETRIES;
		long success = responseStats.getCount(KVSResponseStatus.SUCCESS, null, -1);
		long timeout = responseStats.getCount(KVSResponseStatus.TIMEOUT, null, -1);
		long error_kvs = responseStats.getCount(KVSResponseStatus.ERROR_KVS, null, -1);
		long error_uid = responseStats.getCount(KVSResponseStatus.ERROR_UID, null, -1);
		long error = error_kvs + error_uid;

		long max_rt = responseStats.getMaxResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		long min_rt = responseStats.getMinResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		double avg_rt = responseStats.getAvgResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		double stdev_rt = responseStats.getStdevResponseTime(KVSResponseStatus.SUCCESS, null, -1);

		int clients = (responseStats.getClientCount() == 0 ? numberOfClients : responseStats.getClientCount());
		
		if (responseStats.getClientCount() < 1) {
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Goodput (Requests per Second)",
					-1.0));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Maximum Response Time (ms)",
					-1.0));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Average Response Time (ms)",
					-1.0));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) STDEV Response Time (ms)",
					-1.0));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Total Requests (without replies)",
					Long.toString(-1)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Total Requests (including replies)",
					Long.toString(-1)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Successful Responses",
					Long.toString(-1)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Timeout Responses",
					Long.toString(-1)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Total Failed Responses",
					Long.toString(-1)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) % Success",
					Long.toString(-1)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) % Timeout",
					Long.toString(-1)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) % Failed",
					Long.toString(-1)));			
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) CPU load (loadavg - last one minute)",
					"-1"));
		} else {	
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Goodput (Requests per Second)",
					Math.ceil((double) 1000 * success / testDuration)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Maximum Response Time (ms)",
					Math.ceil(max_rt)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Average Response Time (ms)",
					Math.ceil(avg_rt)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) STDEV Response Time (ms)",
					Math.ceil(stdev_rt)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Total Requests (without replies)",
					Long.toString(total)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Total Requests (including replies)",
					Long.toString(retries)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Successful Responses",
					Long.toString(success)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Timeout Responses",
					Long.toString(timeout)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) Total Failed Responses",
					Long.toString(error)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) % Success",
					getPercentage(success, total)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) % Timeout",
					getPercentage(timeout, total)));
			resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) % Failed",
					getPercentage(error, total)));
			if (!cpuLoadAvg.equalsIgnoreCase("NA")) {
				resultMap.add(new KVSResultField(testName + ": " + clients + " Client(s) CPU load (loadavg - last one minute)",
						cpuLoadAvg));
			}			
		}

		System.out.println(
//				"---------------------------------------" + String.format("%n") +
				testName + ": Test Status: " + testStatus + String.format("%n") +
						"Clients: " + responseStats.getClientCount()
		);

		if (total != 0) {
			printCountStats(total, retries, success, timeout, error, error_kvs, error_uid);
			printResponseTimeStats(max_rt, min_rt, avg_rt, stdev_rt);
			System.out.println("Throughput: " + (double) 1000 * total / testDuration + " requests per second");
			System.out.println("Goodput: " + (double) 1000 * success / testDuration + " requests per second");
			System.out.println("Retry Rate (Successful requests): " + (double) retries / total);
		}
		else {
			System.out.println("No packets observed during test!");
		}
	}

	public void printAggregateSummaryStats(ArrayList<KVSResultField> resultMap) {
		long total = responseStats.getCount(null, null, -1);
		long retries = responseStats.COUNT_RETRIES;
		long success = responseStats.getCount(KVSResponseStatus.SUCCESS, null, -1);
		long timeout = responseStats.getCount(KVSResponseStatus.TIMEOUT, null, -1);
		long error_kvs = responseStats.getCount(KVSResponseStatus.ERROR_KVS, null, -1);
		long error_uid = responseStats.getCount(KVSResponseStatus.ERROR_UID, null, -1);
		long error = error_kvs + error_uid;

		long max_rt = responseStats.getMaxResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		long min_rt = responseStats.getMinResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		double avg_rt = responseStats.getAvgResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		double stdev_rt = responseStats.getStdevResponseTime(KVSResponseStatus.SUCCESS, null, -1);

		int clients = responseStats.getClientCount();
		resultMap.add(new KVSResultField(clients + " Client(s) Throughput (Requests per Second)",
				Math.ceil((double) 1000 * total / testDuration)));
		resultMap.add(new KVSResultField(clients + " Client(s) Goodput (Requests per Second)",
				Math.ceil((double) 1000 * success / testDuration)));
		resultMap.add(new KVSResultField(clients + " Client(s) Maximum Response Time (ms)",
				Math.ceil(max_rt)));
		resultMap.add(new KVSResultField(clients + " Client(s) Average Response Time (ms)",
				Math.ceil(avg_rt)));
		resultMap.add(new KVSResultField(clients + " Client(s) STDEV Response Time (ms)",
				Math.ceil(stdev_rt)));

		System.out.println(
//				"---------------------------------------" + String.format("%n") +
				"Test Status: " + testStatus + String.format("%n") +
						"Clients: " + responseStats.getClientCount()
		);

		if (total != 0) {
			printCountStats(total, retries, success, timeout, error, error_kvs, error_uid);
			printResponseTimeStats(max_rt, min_rt, avg_rt, stdev_rt);
			System.out.println("Throughput: " + (double) 1000 * total / testDuration + " requests per second");
			System.out.println("Goodput: " + (double) 1000 * success / testDuration + " requests per second");
			System.out.println("Retry Rate (Successful requests): " + (double) retries / total);
		}
		else {
			System.out.println("No packets observed during test!");
		}
	}

	public void printAggregateSummaryStats() {
		long total = responseStats.getCount(null, null, -1);
		long retries = responseStats.COUNT_RETRIES;
		long success = responseStats.getCount(KVSResponseStatus.SUCCESS, null, -1);
		long timeout = responseStats.getCount(KVSResponseStatus.TIMEOUT, null, -1);
		long error_kvs = responseStats.getCount(KVSResponseStatus.ERROR_KVS, null, -1);
		long error_uid = responseStats.getCount(KVSResponseStatus.ERROR_UID, null, -1);
		long error = error_kvs + error_uid;

		long max_rt = responseStats.getMaxResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		long min_rt = responseStats.getMinResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		double avg_rt = responseStats.getAvgResponseTime(KVSResponseStatus.SUCCESS, null, -1);
		double stdev_rt = responseStats.getStdevResponseTime(KVSResponseStatus.SUCCESS, null, -1);

		System.out.println(
//				"---------------------------------------" + String.format("%n") +
						"Test Status: " + testStatus + String.format("%n") +
						"Clients: " + responseStats.getClientCount()
		);

		if (total != 0) {
			printCountStats(total, retries, success, timeout, error, error_kvs, error_uid);
			printResponseTimeStats(max_rt, min_rt, avg_rt, stdev_rt);
			System.out.println("Throughput: " + (double) 1000 * total / testDuration + " requests per second");
			System.out.println("Goodput: " + (double) 1000 * success / testDuration + " requests per second");
			System.out.println("Retry Rate (Successful requests): " + (double) retries / total);
		}
		else {
			System.out.println("No packets observed during test!");
		}
	}

	public void writeDetailedResponseInfo(String fileName) {
		responseStats.writeDetailedResponseInfo(fileName);
	}
}
