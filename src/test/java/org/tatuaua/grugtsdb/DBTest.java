package org.tatuaua.grugtsdb;

import org.junit.jupiter.api.*;
import org.tatuaua.grugtsdb.model.Field;
import org.tatuaua.grugtsdb.model.FieldType;
import org.tatuaua.grugtsdb.model.ReadResponse;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DBTest {

    @BeforeEach
    void setUp() throws IOException {
        // Clear the database directory before each test
        DB.BUCKET_METADATA_MAP.clear();
        DB.DIR.delete();
        DB.DIR.mkdir();
    }

    @Test
    void testCreateBucket() throws IOException {
        String bucketName = "testBucket";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        DB.createBucket(bucketName, fields);

        assertTrue(DB.BUCKET_METADATA_MAP.containsKey(bucketName));
        assertEquals(fields, DB.BUCKET_METADATA_MAP.get(bucketName).getFields());
    }

    @Test
    void testWriteToBucket() throws IOException {
        String bucketName = "testBucket2";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        DB.createBucket(bucketName, fields);

        Map<String, Object> fieldValues = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 42
        );

        DB.writeToBucket(bucketName, fieldValues);

        assertEquals(1, DB.BUCKET_METADATA_MAP.get(bucketName).getRecordAmount());
    }

    @Test
    void testReadMostRecent() throws IOException {
        String bucketName = "testBucket3";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        DB.createBucket(bucketName, fields);

        Map<String, Object> fieldValues1 = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 42
        );

        Map<String, Object> fieldValues2 = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 84
        );

        DB.writeToBucket(bucketName, fieldValues1);
        DB.writeToBucket(bucketName, fieldValues2);

        ReadResponse response = DB.readMostRecent(bucketName);

        assertEquals(84, response.getData().get("value"));
    }

    @Test
    void testReadAll() throws IOException {
        String bucketName = "testBucket4";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        DB.createBucket(bucketName, fields);

        Map<String, Object> fieldValues1 = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 42
        );

        Map<String, Object> fieldValues2 = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 84
        );

        DB.writeToBucket(bucketName, fieldValues1);
        DB.writeToBucket(bucketName, fieldValues2);

        List<ReadResponse> responses = DB.readAll(bucketName);

        assertEquals(2, responses.size());
        assertEquals(42, responses.get(0).getData().get("value"));
        assertEquals(84, responses.get(1).getData().get("value"));
    }

    @Test
    void testWriteSpeed() throws IOException {
        String bucketName = "speedTestBucket";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.LONG, 8) // Using LONG to potentially store larger counts if needed
        );

        DB.createBucket(bucketName, fields);

        Map<String, Object> fieldValues = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 0L
        );

        int numWrites = 10000;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numWrites; i++) {
            DB.writeToBucket(bucketName, fieldValues);
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println(String.format("Wrote %d records in %d ms (%f records/ms)",
                numWrites, duration, (double) numWrites / duration));

        assertEquals(numWrites, DB.BUCKET_METADATA_MAP.get(bucketName).getRecordAmount());
        assertTrue(duration < 5000, "Write speed is slower than expected (adjust threshold as needed)"); // Example threshold
    }

    @Test
    void testReadInTimeRange() throws IOException {
        String bucketName = "testBucket5";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        DB.createBucket(bucketName, fields);

        long now = System.currentTimeMillis();

        Map<String, Object> fieldValues1 = Map.of(
                "timestamp", now - 1000,
                "value", 42
        );

        Map<String, Object> fieldValues2 = Map.of(
                "timestamp", now + 1000,
                "value", 84
        );

        DB.writeToBucket(bucketName, fieldValues1);
        DB.writeToBucket(bucketName, fieldValues2);

        List<ReadResponse> responses = DB.readInTimeRange(bucketName, now - 2000, now);

        assertEquals(1, responses.size());
        assertEquals(42, responses.get(0).getData().get("value"));
    }
}