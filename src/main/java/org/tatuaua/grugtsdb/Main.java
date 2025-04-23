/*package org.tatuaua.grugtsdb;

import lombok.extern.slf4j.Slf4j;
import org.tatuaua.grugtsdb.model.Field;
import org.tatuaua.grugtsdb.model.FieldType;
import org.tatuaua.grugtsdb.model.ReadResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.tatuaua.grugtsdb.Engine.*;
import static org.tatuaua.grugtsdb.Engine.createBucket;

@Slf4j
public class Main {

    public static void testWriteToBucket(String bucketName, Map<String, Object> dataToWrite) throws IOException {
        writeToBucket(bucketName, dataToWrite);
        log.info("Data written to bucket {}: {}", bucketName, dataToWrite);
    }

    public static void testReadFromBucketAndConfirm(String bucketName, Map<String,Object> expectedData) throws IOException{
        ReadResponse readResponse = readMostRecent(bucketName);
        log.info("Data read from bucket {}: {}", bucketName, readResponse);
        confirmWriteSuccessful(expectedData, readResponse.getData(), bucketName);
    }

    private static void confirmWriteSuccessful(Map<String, Object> expectedData, Map<String, Object> actualData, String bucketName) {
        if (!expectedData.get("field").equals(actualData.get("field"))) {
            String errorMessage = String.format(
                    "Write confirmation failed for bucket %s.  Expected: %s, Actual: %s",
                    bucketName, expectedData, actualData
            );
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        log.info("Write to bucket {} confirmed successful.", bucketName);
    }

    public static void main(String[] args) throws IOException {
        createBucket("balls", List.of(new Field("timestamp", FieldType.LONG, 0), new Field("field", FieldType.INT, 0))); // Removed direct call to createBucket
        String bucketName = "balls";
        Map<String, Object> dataToWrite = new HashMap<>();
        dataToWrite.put("timestamp", System.currentTimeMillis());
        dataToWrite.put("field", 123);

        //testWriteToBucket(bucketName, dataToWrite);
        testReadFromBucketAndConfirm(bucketName, dataToWrite);
    }

    public static void c() throws IOException {
        createBucket("balls", List.of(new Field("timestamp", FieldType.LONG, 0), new Field("field", FieldType.INT, 0)));
    }
}*/