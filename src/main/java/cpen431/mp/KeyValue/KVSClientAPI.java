package cpen431.mp.KeyValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import cpen431.mp.RequestReply.RRClientAPI;
import cpen431.mp.Statistics.KVSClientLog;
import cpen431.mp.Tests.Tests;
import cpen431.mp.Utilities.ServerNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import cpen431.mp.ProtocolBuffers.KeyValueRequest;
import cpen431.mp.ProtocolBuffers.KeyValueResponse;

public class KVSClientAPI {
	private static final int MAX_KEY_LENGTH = 32;
	private static final int MIN_VALUE_LENGTH = 1;
	private static final int MAX_VALUE_LENGTH = 10000;

	private static Random randInt = new Random();

	private static int getRandomInteger(int min, int max, Random rand) {
		return rand.nextInt((max - min) + 1) + min;
	}
	
	// Get application protocol-level status
	private static KVSErrorCode getErrorCode(int recv) {
		KVSErrorCode errorCode;

		switch(recv){
		case 0:
			errorCode = KVSErrorCode.SUCCESS;
			break;
		case 1:
			errorCode = KVSErrorCode.ERROR_NO_KEY;
			break;
		case 2:
			errorCode = KVSErrorCode.ERROR_OUT_OF_SPACE;
			break;
		case (byte)3:
			errorCode = KVSErrorCode.ERROR_OVERLOAD;
			break;
		case 4:
			errorCode = KVSErrorCode.ERROR_INTERNAL_FAILURE;
			break;
		case 5:
			errorCode = KVSErrorCode.ERROR_UNRECOGNIZED_COMMAND;
			break;
		case 6:
			errorCode = KVSErrorCode.ERROR_INVALID_KEY_LENGTH;
			break;
		case 7:
			errorCode = KVSErrorCode.ERROR_INVALID_VALUE_LENGTH;
			break;
		default:
			errorCode = KVSErrorCode.ERROR_UNKNOWN;
			break;
		}

		return errorCode;
	}

	// Get request-reply protocol-level status
	private static KVSResponseStatus getResponseStatus(RRClientAPI rrClient, int errCode) {
		KVSResponseStatus status;

		// Check if timed out
		if (rrClient.isTimeout()) {
			status = KVSResponseStatus.TIMEOUT;
		}
		// Check that the response has the same uid as the request
		else if (!rrClient.isValidChecksum()) {
			status = KVSResponseStatus.ERROR_CHECKSUM;
		}
		else if (!rrClient.isValidUID()) {
			status = KVSResponseStatus.ERROR_UID;
		}
		// Check that there is any application-level error
		else if (errCode != 0) {
			status = KVSResponseStatus.ERROR_KVS;
        } else {
			status = KVSResponseStatus.SUCCESS;
        }

		return status;
	}

	private static byte[] packPayload(KVSRequest request) {
		byte[] payload = null;

		switch(request.type) {
			case PUT:
				payload = KeyValueRequest.KVRequest.newBuilder()
						.setCommand(1)
						.setKey(ByteString.copyFrom(request.key))
						.setValue(ByteString.copyFrom(request.value))
						.build().toByteArray();
				break;
			case GET:
				payload = KeyValueRequest.KVRequest.newBuilder()
						.setCommand(2)
						.setKey(ByteString.copyFrom(request.key))
						.build().toByteArray();
				break;
			case REMOVE:
				payload = KeyValueRequest.KVRequest.newBuilder()
						.setCommand(3)
						.setKey(ByteString.copyFrom(request.key))
						.build().toByteArray();
				break;
			case SHUTDOWN:
				payload = KeyValueRequest.KVRequest.newBuilder()
						.setCommand(4)
						.build().toByteArray();
				break;
			case WIPEOUT:
				payload = KeyValueRequest.KVRequest.newBuilder()
						.setCommand(5)
						.build().toByteArray();
				break;
			case ISALIVE:
				payload = KeyValueRequest.KVRequest.newBuilder()
						.setCommand(6)
						.build().toByteArray();
				break;
			case GETPID:
				payload = KeyValueRequest.KVRequest.newBuilder()
						.setCommand(7)
						.build().toByteArray();
				break;
			case INVALID:
				payload = KeyValueRequest.KVRequest.newBuilder()
						.setCommand(99)
						.build().toByteArray();
				break;
			default:
				throw new RuntimeException("Invalid Request Type!");
		}

		return payload;
	}

	private static KVSResponse unpackResponse(RRClientAPI rrClient, byte[] recv, KVSRequest request) {
		KVSResponse response = new KVSResponse();
		response.requestType = request.type;
		response.key = request.key;
		response.value = request.value;
		response.responseTime = rrClient.getResponseTime();
		response.retries = rrClient.getNumberOfAttempts();
		response.requestUID = rrClient.getResponseMessageID();

		try {
			if (rrClient.isTimeout()) {
				response.status = KVSResponseStatus.TIMEOUT;
				return response;
			}

			if (!rrClient.isValidChecksum()) {
				response.status = KVSResponseStatus.ERROR_CHECKSUM;
				return response;
			}

			if (!rrClient.isValidUID()) {
				response.status = KVSResponseStatus.ERROR_UID;
				return response;
			}

			KeyValueResponse.KVResponse kvres = KeyValueResponse.KVResponse.parseFrom(recv);

			// Check Response Status and Application Error Code
			response.errorCode = getErrorCode(kvres.getErrCode());
			response.status = getResponseStatus(rrClient, kvres.getErrCode());

			// Get value from response
			switch(request.type) {
				case GET:
					if (response.status == KVSResponseStatus.SUCCESS) {
						response.value = kvres.getValue().toByteArray();
					}
					break;
				case GETPID:
					if (response.status == KVSResponseStatus.SUCCESS) {
						response.pid = kvres.getPid();
					}
					break;
			}
		} catch (InvalidProtocolBufferException e) {
			response.status = KVSResponseStatus.ERROR_INVALID_FORMAT;
			response.errorCode = KVSErrorCode.ERROR_UNRECOGNIZED_COMMAND;
		}

		return response;
	}

	private static KVSResponse sendRequest(KVSRequest request) {
		return sendRequest(request, -1);
	}

	private static KVSResponse sendRequest(KVSRequest request, int clientID) {
		// Pick a random server from the distributed KVS
		int randomInteger = getRandomInteger(0, request.serverNodes.size() - 1, randInt);
		ServerNode sn = request.serverNodes.get(randomInteger);

		// Setup Connector
		RRClientAPI rrClient = new RRClientAPI(sn.getHostName(), sn.getPortNumber());

		// Check key and value lengths
		if (request.key != null && request.key.length > MAX_KEY_LENGTH) {
			throw new RuntimeException("Invalid Key Length" + request.key.length);
		} else if (request.value != null && request.value.length > MAX_VALUE_LENGTH) {
			throw new RuntimeException("Invalid Value Length:" + request.value.length);
		}

		// Send request
		byte[] payload = packPayload(request);
		byte[] recv = rrClient.sendAndReceive(payload);

		request.requestUID = rrClient.getRequestMessageID();
		rrClient.close();

		KVSResponse response = unpackResponse(rrClient, recv, request);

		// client logging
		if (KVSClientLog.enableLogging) {
			// this logging method here will not work with requests parallelized over the remote nodes, e.g., is alive, wipeout, get pid
			if ((request.type == KVSRequestType.PUT || request.type == KVSRequestType.GET)) {
				KVSClientLog clientLog = KVSClientLog.kvsClientLogs[clientID];

				if (KVSClientLog.logAll) {
					KVSClientLog.MessageExchange me = clientLog.new MessageExchange(System.currentTimeMillis(),
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((response.requestUID == null ? "null".getBytes() : response.requestUID))),
							request.type,
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((response.key == null ? "null".getBytes() : response.key))),
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((request.value == null ? "null".getBytes() : request.value))),
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((response.value == null ? "null".getBytes() : response.value))),
							response.errorCode,
							response.status);
					clientLog.messageExchanges.add(me);
				}

				if (KVSClientLog.logError && (response.status != KVSResponseStatus.SUCCESS)) {
					KVSClientLog.MessageExchange me = clientLog.new MessageExchange(System.currentTimeMillis(),
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((response.requestUID == null ? "null".getBytes() : response.requestUID))),
							request.type,
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((response.key == null ? "null".getBytes() : response.key))),
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((request.value == null ? "null".getBytes() : request.value))),
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((response.value == null ? "null".getBytes() : response.value))),
							response.errorCode,
							response.status);
					clientLog.messageExchangesError.add(me);
				}

				if (KVSClientLog.logErrorCapacityTest && (response.status != KVSResponseStatus.SUCCESS)) {
					KVSClientLog.MessageExchange me = clientLog.new MessageExchange(System.currentTimeMillis(),
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((response.requestUID == null ? "null".getBytes() : response.requestUID))),
							request.type,
							Tests.toHumanReadableString(DatatypeConverter.printHexBinary((response.key == null ? "null".getBytes() : response.key))),
							response.errorCode,
							response.status);
					clientLog.messageExchangesError.add(me);
				}
			}
		}

		return response;
	}

	public static byte[] intToByte(int i) {
		return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(i).array();
	}

	public static int byteToInt(byte [] b){
		try {
			if (b.length != 4) {
				System.err.println("Invalid value length! Expecting 4 Bytes, received " + b.length + " Bytes.");
			}

			return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getInt();
		}
		catch (Exception e) {
			return Integer.MIN_VALUE;
		}
	}
	
	public static KVSResponse put(ArrayList<ServerNode> serverNodes, byte[] key, byte[] value) {
		 return put(serverNodes, key, value, -1);
	}	
	
	public static KVSResponse put(ArrayList<ServerNode> serverNodes, byte[] key, byte[] value, int clientID) {
		KVSRequest request = new KVSRequest();
		request.type = KVSRequestType.PUT;
		request.serverNodes = serverNodes;
		request.key = key;
		request.value = value;

		return sendRequest(request, clientID);
	}

	public static KVSResponse put(ArrayList<ServerNode> serverNodes, int key, int value) {
		return put(serverNodes, intToByte(key), intToByte(value));
	}

	public static KVSResponse get(ArrayList<ServerNode> serverNodes, byte[] key) {
		return get(serverNodes, key, -1);
	}
	
	public static KVSResponse get(ArrayList<ServerNode> serverNodes, byte[] key, int clientID) {
		KVSRequest request = new KVSRequest();
		request.type = KVSRequestType.GET;
		request.serverNodes = serverNodes;
		request.key = key;

		return sendRequest(request, clientID);
	}

	public static KVSResponse get(ArrayList<ServerNode> serverNodes, int key) {
		return get(serverNodes, intToByte(key));
	}

	public static KVSResponse remove(ArrayList<ServerNode> serverNodes, byte[] key) {
		KVSRequest request = new KVSRequest();
		request.type = KVSRequestType.REMOVE;
		request.serverNodes = serverNodes;
		request.key = key;

		return sendRequest(request);
	}

	public static KVSResponse remove(ArrayList<ServerNode> serverNodes, int key) {
		return remove(serverNodes, intToByte(key));
	}

	public static KVSResponse wipeout(ArrayList<ServerNode> serverNodes) {
		KVSRequest request = new KVSRequest();
		request.type = KVSRequestType.WIPEOUT;
		request.serverNodes = serverNodes;

		return sendRequest(request);
	}

	public static KVSResponse shutdown(ArrayList<ServerNode> serverNodes) {
		KVSRequest request = new KVSRequest();
		request.type = KVSRequestType.SHUTDOWN;
		request.serverNodes = serverNodes;

		return sendRequest(request);
	}

	public static KVSResponse invalid(ArrayList<ServerNode> serverNodes) {
		KVSRequest request = new KVSRequest();
		request.type = KVSRequestType.INVALID;
		request.serverNodes = serverNodes;

		return sendRequest(request);
	}
	
	public static KVSResponse getPID(ArrayList<ServerNode> serverNodes) {
		KVSRequest request = new KVSRequest();
		request.type = KVSRequestType.GETPID;
		request.serverNodes = serverNodes;

		return sendRequest(request);
	}

	public static KVSResponse isAlive(ArrayList<ServerNode> serverNodes) {
		KVSRequest request = new KVSRequest();
		request.type = KVSRequestType.ISALIVE;
		request.serverNodes = serverNodes;

		return sendRequest(request);
	}
}
