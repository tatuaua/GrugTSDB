package org.tatuaua.grugtsdb;

import java.nio.charset.StandardCharsets;

public class Utils {

    public static byte[] stringTo256ByteArray(String input) {
        byte[] stringBytes = (input != null ? input : "").getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[256];
        int bytesToCopy = Math.min(stringBytes.length, 256);
        System.arraycopy(stringBytes, 0, result, 0, bytesToCopy);
        return result;
    }

    public static String byteArray256ToString(byte[] input) {
        if (input == null || input.length != 256) {
            return "";
        }

        int length = 0;
        for (int i = 0; i < 256; i++) {
            if (input[i] == 0) {
                break;
            }
            length = i + 1;
        }

        return new String(input, 0, length, StandardCharsets.UTF_8);
    }
}
