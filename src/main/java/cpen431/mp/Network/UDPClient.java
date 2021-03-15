package cpen431.mp.Network;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;

public class UDPClient {
    private DatagramSocket _socket = null;
    private static final int MAX_BUFFER_SIZE = 65535;

    private long _responseTime = 0;
    private boolean _timeout = false;

    private InetAddress _serverHost;
    private int _serverPort;

    private void setupSocket(String host, int port) {
        try {
            _socket = new DatagramSocket();
        } catch (SocketException e) {
            System.err.println("Could not create _socket!");
        }

        try {
            _serverHost = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            System.err.println("Could not get server host address!");
        }

        _serverPort = port;
    }

    public UDPClient(String host, int port) {
        setupSocket(host, port);
    }

    public void close() {
        if (_socket != null) {
            _socket.close();
            _socket = null;
        }
    }
    public byte[] sendAndReceive(byte[] requestBuffer) {
        _responseTime = 0;
        _timeout = false;

        long sendTime = 0;
        DatagramPacket requestPacket = new DatagramPacket(requestBuffer, requestBuffer.length,
                _serverHost, _serverPort);

        byte[] responseBuffer = new byte[MAX_BUFFER_SIZE];
        Arrays.fill(responseBuffer, (byte) 0);
        DatagramPacket responsePacket = new DatagramPacket(responseBuffer, responseBuffer.length);

        try {
            sendTime = System.currentTimeMillis();
            _socket.send(requestPacket);
        } catch (IOException e) {
            System.err.println("Could not send request packet!");
            return null;
        }

        try {
            _socket.receive(responsePacket);
            _responseTime = System.currentTimeMillis() - sendTime;
            return Arrays.copyOf(responsePacket.getData(), responsePacket.getLength());
        } catch (SocketTimeoutException e) {
            _timeout = true;
            return null;
        } catch (IOException e) {
            System.err.println("Could not send request packet!");
            return null;
        }
    }

    public void setTimeout(int duration) {
        if (duration >= 0) {
            try {
                _socket.setSoTimeout(duration);
            } catch (SocketException e) {
                System.err.println("Could not set socket timeout!");
            }
        }
    }

    public long getResponseTime() {
        return _responseTime;
    }

    public boolean isTimeout() {
        return _timeout;
    }

    public byte[] getLocalAddress() {
        return _socket.getLocalAddress().getAddress();
    }

    public int getLocalPort() {
        return _socket.getLocalPort();
    }

}
