package cpen431.mp.Utilities;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;

public class Checksum {
    public static long generate(byte[] first, byte[] second) {
        java.util.zip.Checksum checksum = new CRC32();

        ByteBuffer byteBuffer = ByteBuffer.allocate(first.length + second.length);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.put(first);
        byteBuffer.put(second);
        byte[] msgBytes = byteBuffer.array();

        checksum.update(msgBytes, 0, msgBytes.length);

        return checksum.getValue();
    }

    public static String getMD5(String message) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(message.getBytes(), 0, message.length());

            BigInteger i = new BigInteger(1, digest.digest());
            return String.format("%1$032X", i);
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }
}
