package cpen431.mp.Network;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by Simon on 17.03.17.
 */
public class TCPClient {;

    public static byte[] sendAndReceive(byte[] payload){

        try
        {
            String host = Config.POP_HOSTNAME;
            int port = Config.POP_PORT;
            InetAddress address = InetAddress.getByName(host);
            Socket socket = new Socket(address, port);

            //Send the message to the server
            OutputStream os = socket.getOutputStream();
            DataOutputStream dos = new DataOutputStream(os);
            dos.writeInt(payload.length);
            dos.write(payload);

            System.out.println("Message sent to the server! ");

            //Get the return message from the server
            DataInputStream din = new DataInputStream(socket.getInputStream());
            int length = din.readInt();
            if(length > 0){
                byte[] message = new byte[length];
                din.readFully(message,0,message.length);
                socket.close();
                return message;
            }
            socket.close();
            return null;

        }
        catch (Exception exception)
        {
            System.err.println("Error while connectiing/sending/receiving to server");
            exception.printStackTrace();
        }
        return null;

    }
}