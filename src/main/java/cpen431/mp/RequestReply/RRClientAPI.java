package cpen431.mp.RequestReply;

import cpen431.mp.Network.UDPClient;
import cpen431.mp.ProtocolBuffers.Message.Msg;
import cpen431.mp.Utilities.Checksum;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class RRClientAPI {
	private static final int INITIAL_REQUEST_TIMEOUT = 400;
	private static final int INITIAL_RETRY_TIMEOUT = 1000;
	private static final int MAX_RETRY_ATTEMPTS = 3;
	private static final int UID_SIZE = 16;

	private int _attempt = 0;
	private long _responseTime = 0;
	private boolean _timeout = false;
	private boolean _validUID = false;
	private boolean _validChecksum = false;
	private byte[] _requestMessageID = null;
	private byte[] _responseMessageID = null;
	private UDPClient _udpClient;

	private int _getTimeoutDuration(int attempt) {
		if (attempt <= 1) {
			return INITIAL_REQUEST_TIMEOUT;
		} else if (attempt == 2) {
			return INITIAL_RETRY_TIMEOUT;
		} else {
			return INITIAL_REQUEST_TIMEOUT * (attempt - 2) * 2;
		}
	}

	private byte[] _unpackMessage(byte[] responsePayload) {
		try {
			Msg responseMsg = Msg.parseFrom(responsePayload);
			_responseMessageID = responseMsg.getMessageID().toByteArray();
			byte[] responseApplicationPayload = responseMsg.getPayload().toByteArray();
			long expectedChecksum = Checksum.generate(_responseMessageID, responseApplicationPayload);

			if(responseMsg.getCheckSum() == expectedChecksum) {
				_validChecksum = true;
			} else {
				return null;
			}

			if (Arrays.equals(_responseMessageID, _requestMessageID)) {
				_validUID = true;
			} else {
				return null;
			}

			return responseApplicationPayload;
		} catch (InvalidProtocolBufferException e) {
			System.err.println("Unable to deserialize response!");
			return null;
		}
	}

	public RRClientAPI(String host, int port) {
		_udpClient = new UDPClient(host, port);
	}

	public void close() {
		_udpClient.close();
	}

	public byte[] sendAndReceive(byte[] applicationPayload, byte[] messageID) {
		_requestMessageID = messageID;
		_attempt = 0;
		long requestChecksum = Checksum.generate(_requestMessageID, applicationPayload);

		byte[] requestPayload = Msg.newBuilder()
				.setMessageID(ByteString.copyFrom(_requestMessageID))
				.setPayload(ByteString.copyFrom(applicationPayload))
				.setCheckSum(requestChecksum)
				.build().toByteArray();

		byte[] responseApplicationPayload = null;
		while (_attempt < (MAX_RETRY_ATTEMPTS + 1)) {
			_attempt++;
			_timeout = false;
			_responseTime = 0;
			_validUID = false;
			_validChecksum = false;

			_udpClient.setTimeout(_getTimeoutDuration(_attempt));
			byte[] responsePayload = _udpClient.sendAndReceive(requestPayload);
			_timeout = _udpClient.isTimeout();
			_responseTime = _udpClient.getResponseTime();

			if (_timeout) {
				continue;
			}

			responseApplicationPayload = _unpackMessage(responsePayload);
			if(responseApplicationPayload == null) {
				continue;
			} else {
				break;
			}
		}

		return responseApplicationPayload;
	}

	public byte[] sendAndReceive(byte[] applicationPayload) {
		return sendAndReceive(applicationPayload, generateFullyRandomUID(UID_SIZE));
	}

	private byte[] generateFullyRandomUID(int size) {
		byte[] uid = new byte[size];
		ThreadLocalRandom.current().nextBytes(uid);

		return uid;
	}

	private byte[] generateA1RandomUID(int size) {
		byte[] uid = new byte[size];
		ThreadLocalRandom.current().nextBytes(uid);

		try {
			byte[] ip = _udpClient.getLocalAddress();
			byte[] port = ByteBuffer.allocate(2).putShort(new Integer(_udpClient.getLocalPort()).shortValue()).array();
			byte[] time = ByteBuffer.allocate(8).putLong(System.nanoTime()).array();

			for (int i = 0; i < 4; i++) {
				uid[i] = ip[i];
			}

			for (int i = 0; i < 2; i++) {
				uid[i + 4] = port[i];
			}

			for (int i = 2; i < 8; i++) {
				uid[i + 8] = time[i];
			}
		} catch (Exception e) {
			System.err.println("Error generating unique message ID in the required format!");
		}

		return uid;
	}

	public byte[] getRequestMessageID() {
		return _requestMessageID;
	}

	public byte[] getResponseMessageID() {
		return _responseMessageID;
	}

	public long getResponseTime() {
		return _responseTime;
	}

	public int getNumberOfAttempts() {
		return _attempt;
	}

	public boolean isTimeout() {
		return _timeout;
	}

	public boolean isValidUID() {
		return _validUID;
	}

	public boolean isValidChecksum() {
		return _validChecksum;
	}
}
