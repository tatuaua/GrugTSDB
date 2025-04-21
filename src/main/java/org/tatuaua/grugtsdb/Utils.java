package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.tatuaua.grugtsdb.model.BucketMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static byte[] stringTo256ByteArray(String input) {
        byte[] stringBytes = (input != null ? input : "").getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[256];
        int bytesToCopy = Math.min(stringBytes.length, 256);
        System.arraycopy(stringBytes, 0, result, 0, bytesToCopy);
        return result;
    }

    public static byte[] stringToByteArray(String input, int bufferSize) {
        byte[] stringBytes = (input != null ? input : "").getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[bufferSize];
        int bytesToCopy = Math.min(stringBytes.length, bufferSize);
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

    public static String byteArrayToString(byte[] input) {
        if (input == null) {
            return "";
        }

        int length = 0;
        for (int i = 0; i < input.length; i++) {
            if (input[i] == 0) {
                break;
            }
            length = i + 1;
        }

        return new String(input, 0, length, StandardCharsets.UTF_8);
    }

    public static List<BucketMetadata> readBucketMetadata(File dir) {
        List<BucketMetadata> metadataList = new ArrayList<>();
        Path directory = Paths.get(dir.getName());
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory,
                entry -> Files.isRegularFile(entry) && entry.getFileName().toString().toLowerCase().endsWith(".grug_meta"))) {
            for (Path entry : stream) {
                try {
                    metadataList.add(MAPPER.readValue(Files.readAllBytes(entry), BucketMetadata.class));
                    System.out.println("Read: " + entry.getFileName());
                } catch (IOException e) {
                    System.err.println("Error reading/deserializing: " + entry.getFileName() + " - " + e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return metadataList;
    }
}
