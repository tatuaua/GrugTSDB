package org.tatuaua.grugtsdb;

import org.junit.jupiter.api.*;
import org.tatuaua.grugtsdb.engine.Engine;
import org.tatuaua.grugtsdb.engine.model.Field;
import org.tatuaua.grugtsdb.engine.model.FieldType;
import org.tatuaua.grugtsdb.engine.model.ReadResponse;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class EngineTest {

    @BeforeAll
    @AfterAll
    static void clearDatabase() {
        Engine.clearMetadata();
        Engine.clearDatabase();
        log.info("Database cleared.");
    }

    // Simulates starting the engine
    @BeforeEach
    void generateMetadata() {
        Engine.generateMetadata();
    }

    // Simulates stopping the engine
    @AfterEach
    void clearMetadata() {
        Engine.clearMetadata();
    }

    @Test
    void testCreateBucket() throws IOException {
        String bucketName = "testBucket";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        Engine.createBucket(bucketName, fields);

        assertTrue(Engine.BUCKET_METADATA_MAP.containsKey(bucketName));
        assertEquals(fields, Engine.BUCKET_METADATA_MAP.get(bucketName).getFields());
    }

    @Test
    void testWriteToBucket() throws IOException {
        String bucketName = "testBucket2";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        Engine.createBucket(bucketName, fields);

        Map<String, Object> fieldValues = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 42
        );

        Engine.writeToBucket(bucketName, fieldValues);

        assertEquals(1, Engine.BUCKET_METADATA_MAP.get(bucketName).getRecordAmount());
    }

    @Test
    void testReadMostRecent() throws IOException {
        String bucketName = "testBucket3";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        Engine.createBucket(bucketName, fields);

        Map<String, Object> fieldValues1 = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 42
        );

        Map<String, Object> fieldValues2 = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 84
        );

        Engine.writeToBucket(bucketName, fieldValues1);
        Engine.writeToBucket(bucketName, fieldValues2);

        ReadResponse response = Engine.readMostRecent(bucketName);

        assertEquals(84, response.getData().get("value"));
    }

    @Test
    void testReadAll() throws IOException {
        String bucketName = "testBucket4";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        Engine.createBucket(bucketName, fields);

        Map<String, Object> fieldValues1 = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 42
        );

        Map<String, Object> fieldValues2 = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 84
        );

        Engine.writeToBucket(bucketName, fieldValues1);
        Engine.writeToBucket(bucketName, fieldValues2);

        List<ReadResponse> responses = Engine.readAll(bucketName);

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

        Engine.createBucket(bucketName, fields);

        Map<String, Object> fieldValues = Map.of(
                "timestamp", System.currentTimeMillis(),
                "value", 0L
        );

        int numWrites = 10000;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numWrites; i++) {
            Engine.writeToBucket(bucketName, fieldValues);
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        log.info(String.format("Wrote %d records in %d ms (%f records/ms)",
                numWrites, duration, (double) numWrites / duration));

        assertEquals(numWrites, Engine.BUCKET_METADATA_MAP.get(bucketName).getRecordAmount());
    }

    @Test
    void testReadInTimeRange() throws IOException {
        String bucketName = "testBucket5";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        Engine.createBucket(bucketName, fields);

        long now = System.currentTimeMillis();

        Map<String, Object> fieldValues1 = Map.of(
                "timestamp", now - 1000,
                "value", 42
        );

        Map<String, Object> fieldValues2 = Map.of(
                "timestamp", now + 1000,
                "value", 84
        );

        Engine.writeToBucket(bucketName, fieldValues1);
        Engine.writeToBucket(bucketName, fieldValues2);

        List<ReadResponse> responses = Engine.readInTimeRange(bucketName, now - 2000, now);

        assertEquals(1, responses.size());
        assertEquals(42, responses.get(0).getData().get("value"));
    }

    @Test
    void testReadAvgInRange() throws IOException {
        String bucketName = "testBucket6";
        List<Field> fields = List.of(
                new Field("timestamp", FieldType.LONG, 8),
                new Field("value", FieldType.INT, 4)
        );

        Engine.createBucket(bucketName, fields);

        long now = System.currentTimeMillis();

        Map<String, Object> fieldValues1 = Map.of(
                "timestamp", now - 1000,
                "value", 42
        );

        Map<String, Object> fieldValues2 = Map.of(
                "timestamp", now - 999,
                "value", 44
        );

        Map<String, Object> fieldValues3 = Map.of(
                "timestamp", now + 1000,
                "value", 84
        );

        Engine.writeToBucket(bucketName, fieldValues1);
        Engine.writeToBucket(bucketName, fieldValues2);
        Engine.writeToBucket(bucketName, fieldValues3);

        ReadResponse response = Engine.aggregateRead(bucketName, now - 2000, now, "value", "avg");

        assertNotNull(response);
        assertEquals(43.0, response.getData().get("value_avg"));
    }
}