package cpen431.mp.Statistics;

import cpen431.mp.KeyValue.KVSErrorCode;
import cpen431.mp.KeyValue.KVSRequestType;
import cpen431.mp.KeyValue.KVSResponse;
import cpen431.mp.KeyValue.KVSResponseStatus;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class KVSResponseStats {
	private class DetailedResponseInfo {
		int clientID;
		KVSRequestType type;
		KVSResponseStatus status;
		KVSErrorCode errorCode;
		long retries;
		long time;
		boolean validStat = true;

		public DetailedResponseInfo(int clientID, KVSRequestType type, KVSResponseStatus status,
		                            KVSErrorCode errorCode, long retries, long time, boolean validStat) {
			this.clientID = clientID;
			this.type = type;
			this.status = status;
			this.errorCode = errorCode;
			this.retries = retries;
			this.time = time;
			this.validStat = validStat;
		}

		public DetailedResponseInfo(int clientID, KVSResponse response) {
			this(clientID, response.requestType, response.status,
					response.errorCode, response.retries, response.responseTime, response.validStat);
		}

		public String toStringCSV() {
			return (clientID + ", " + type + ", " + status + ", " + errorCode + ", " +  retries + ", " + time +
					", " + validStat);
		}
	}

	// Private data
	private Set<Integer> clientSet = new HashSet<>();
	private ArrayList<DetailedResponseInfo> detailedResponses = new ArrayList<>();

	public long COUNT_SUCCESS = 0;
	public long COUNT_TIMEOUT = 0;
	public long COUNT_ERROR = 0;
	public long COUNT_ERROR_KVS = 0;
	public long COUNT_ERROR_UID = 0;
	public long COUNT_TOTAL = 0;
	public long COUNT_RETRIES = 0;

	public long MAX_RT = Long.MIN_VALUE;
	public long MIN_RT = Long.MAX_VALUE;

	// Private methods
	private Boolean checkFilter(KVSResponseStatus filterStatus, KVSRequestType filterType,
	                            int filterClientID, DetailedResponseInfo response) {
		if (filterStatus != null && filterStatus != response.status) {
			return false;
		}
		else if (filterType != null && filterType != response.type) {
			return false;
		}
		else if (filterClientID >= 0 && filterClientID != response.clientID) {
			return false;
		}

		return true;
	}

	private long getFilteredCount(KVSResponseStatus filterStatus, KVSRequestType filterType, int filterClientID) {
		long count = 0;

		for (DetailedResponseInfo response : detailedResponses) {
			if (response.validStat && checkFilter(filterStatus, filterType, filterClientID, response)) {
				count++;
			}
		}

		return count;
	}

	private long getFilteredRetrySum(KVSResponseStatus filterStatus, KVSRequestType filterType, int filterClientID) {
		long sum = 0;

		for (DetailedResponseInfo response : detailedResponses) {
			if (response.validStat && checkFilter(filterStatus, filterType, filterClientID, response)) {
				sum += response.retries;
			}
		}

		return sum;
	}

	private long getFilteredMaxResponseTime(KVSResponseStatus filterStatus, KVSRequestType filterType,
	                                       int filterClientID) {
		if (detailedResponses.size() == 0) {
			return -1;
		}

		long max = detailedResponses.get(0).time;

		for (DetailedResponseInfo response : detailedResponses) {
			if (response.validStat && checkFilter(filterStatus, filterType, filterClientID, response) &&
					response.time > max) {
				max = response.time;
			}
		}

		return max;
	}

	private long getFilteredMinResponseTime(KVSResponseStatus filterStatus, KVSRequestType filterType,
	                                        int filterClientID) {
		if (detailedResponses.size() == 0) {
			return -1;
		}

		long min = detailedResponses.get(0).time;

		for (DetailedResponseInfo response : detailedResponses) {
			if (response.validStat && checkFilter(filterStatus, filterType, filterClientID, response) &&
					response.time < min) {
				min = response.time;
			}
		}

		return min;
	}

	private double getFilteredAvgResponseTime(KVSResponseStatus filterStatus, KVSRequestType filterType,
	                                        int filterClientID) {
		if (detailedResponses.size() == 0) {
			return -1;
		}

		long sum = 0;
		long count = 0;

		for (DetailedResponseInfo response : detailedResponses) {
			if (response.validStat && checkFilter(filterStatus, filterType, filterClientID, response)) {
				sum += response.time;
				count++;
			}
		}

		return (double)sum / count;
	}

	private double getFilteredStdevResponseTime(KVSResponseStatus filterStatus, KVSRequestType filterType,
	                                        int filterClientID) {
		if (detailedResponses.size() == 0) {
			return -1;
		}

		double sum = 0;
		long count = 0;
		double mean = getFilteredAvgResponseTime(filterStatus, filterType, filterClientID);

		for (DetailedResponseInfo response : detailedResponses) {
			if (response.validStat && checkFilter(filterStatus, filterType, filterClientID, response)) {
				sum += Math.pow((response.time - mean), 2);
				count++;
			}
		}

		return Math.sqrt(sum / count);
	}

	private void updateCounters(KVSResponse response) {
		if (!response.validStat) {
			return;
		}

		switch (response.status) {
			case SUCCESS:
				COUNT_SUCCESS++;
				break;
			case TIMEOUT:
				COUNT_TIMEOUT++;
				break;
			case ERROR_UID:
				COUNT_ERROR++;
				COUNT_ERROR_UID++;
				break;
			case ERROR_KVS:
				COUNT_ERROR++;
				COUNT_ERROR_KVS++;
				break;
			default:
				throw new RuntimeException("Unknown Response Status!");
		}

		COUNT_TOTAL++;
		COUNT_RETRIES += response.retries;

		if (response.responseTime > MAX_RT) {
			MAX_RT = response.responseTime;
		}
		else if (response.responseTime < MIN_RT) {
			MIN_RT = response.responseTime;
		}
	}

	private void processResponse(int clientID, KVSResponse response) {
		detailedResponses.add(new DetailedResponseInfo(clientID, response));
		clientSet.add(clientID);
		updateCounters(response);
	}

	// Constructors
	public KVSResponseStats(){}

	// Public methods
	public void addKVSResponses(int clientID, ArrayList<KVSResponse> responses) {
		for (KVSResponse response : responses) {
			processResponse(clientID, response);
		}
	}

	public void addKVSResponses(ArrayList<KVSResponse> clientResponses[], int clients) {
		for (int i = 0; i < clients; i++) {
			addKVSResponses(i, clientResponses[i]);
		}
	}

	public int getClientCount() {
		return clientSet.size();
	}

	public long getCount(KVSResponseStatus filterStatus, KVSRequestType filterType, int filterClientID) {
		return getFilteredCount(filterStatus, filterType, filterClientID);
	}

	public long getRetrySum(KVSResponseStatus filterStatus, KVSRequestType filterType, int filterClientID) {
		return getFilteredRetrySum(filterStatus, filterType, filterClientID);
	}

	public long getMaxResponseTime(KVSResponseStatus filterStatus, KVSRequestType filterType, int filterClientID) {
		return getFilteredMaxResponseTime(filterStatus, filterType, filterClientID);
	}

	public long getMinResponseTime(KVSResponseStatus filterStatus, KVSRequestType filterType, int filterClientID) {
		return getFilteredMinResponseTime(filterStatus, filterType, filterClientID);
	}

	public double getAvgResponseTime(KVSResponseStatus filterStatus, KVSRequestType filterType, int filterClientID) {
		return getFilteredAvgResponseTime(filterStatus, filterType, filterClientID);
	}

	public double getStdevResponseTime(KVSResponseStatus filterStatus, KVSRequestType filterType, int filterClientID) {
		return getFilteredStdevResponseTime(filterStatus, filterType, filterClientID);
	}

	public void writeDetailedResponseInfo(String fileName) {
		FileWriter fileWriter;

		try {
			fileWriter = new FileWriter(fileName);

			fileWriter.append("Client ID, Request Type, Status, Retries, Response Time, Valid Stat" + String.format("%n"));
			for (DetailedResponseInfo i : detailedResponses) {
				fileWriter.append(i.toStringCSV() + String.format("%n"));
			}

			fileWriter.flush();
			fileWriter.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}