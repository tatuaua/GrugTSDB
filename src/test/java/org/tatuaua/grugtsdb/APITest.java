package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tatuaua.grugtsdb.model.FieldType;
import org.tatuaua.grugtsdb.model.ReadActionType;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class APITest {

    private static final int PORT = 12345;
    private static final int BUFFER_SIZE = 1024;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private UDPServer server;
    private DatagramSocket clientSocket;
    private static final String BUCKET_NAME = "testBucket";
    private static final String TEST_FIELD_NAME = "testField";
    private static final String TEST_STRING_NAME = "testString";
    private static final String TIMESTAMP_NAME = "timestamp";
    private static final int TEST_FIELD_VALUE = 42;
    private static final String TEST_STRING_VALUE = "balls";
    private static final double TIMESTAMP_VALUE = 0.0f;
    private static final String UPDATED_TEST_STRING_VALUE = "updateballs";
    private static final int UPDATED_TEST_FIELD_VALUE = 53;
    private static final int WRITE_AMOUNT = 1000;
    private static final int CONCURRENT_WRITE_AMOUNT = 100000;
    private static final int NUM_THREADS = 3;
    InetAddress serverAddress;

    @BeforeEach
    void setUp() throws IOException {
        server = new UDPServer(PORT);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                server.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        clientSocket = new DatagramSocket();
    }

    // Helper method to send packet and receive response
    private String sendAndReceive(ObjectNode jsonNode) throws IOException {
        byte[] sendData = MAPPER.writeValueAsBytes(jsonNode);
        DatagramPacket sendPacket = new DatagramPacket(
                sendData,
                sendData.length,
                InetAddress.getByName("localhost"),
                PORT
        );
        log.debug("Sending request: {}", MAPPER.writeValueAsString(jsonNode));
        clientSocket.send(sendPacket);

        if (server.isClosed()) {
            log.error("Server is closed, cannot receive response.");
            return null;
        }

        byte[] receiveData = new byte[BUFFER_SIZE];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        clientSocket.receive(receivePacket);
        String response = new String(receivePacket.getData(), 0, receivePacket.getLength()).trim();
        log.debug("Received response: {}", response);
        return response;
    }

    // Helper method to create basic action JSON
    private ObjectNode createActionNode(String actionType) {
        ObjectNode jsonNode = MAPPER.createObjectNode();
        jsonNode.put("actionType", actionType);
        jsonNode.put("bucketName", APITest.BUCKET_NAME);
        return jsonNode;
    }

    @Test
    void testBucketLifecycle() throws IOException {
        // Create bucket
        ObjectNode createNode = createActionNode("createBucket");
        ArrayNode fieldsArray = MAPPER.createArrayNode();
        ObjectNode grugFieldNode = MAPPER.createObjectNode();
        grugFieldNode.put("name", TEST_FIELD_NAME);
        grugFieldNode.put("type", FieldType.INT.name());
        grugFieldNode.put("size", 0);
        fieldsArray.add(grugFieldNode);

        ObjectNode grugFieldNode2 = MAPPER.createObjectNode();
        grugFieldNode2.put("name", TEST_STRING_NAME);
        grugFieldNode2.put("type", FieldType.STRING.name());
        grugFieldNode2.put("size", 50);
        fieldsArray.add(grugFieldNode2);

        ObjectNode grugFieldNode3 = MAPPER.createObjectNode();
        grugFieldNode3.put("name", TIMESTAMP_NAME);
        grugFieldNode3.put("type", FieldType.DOUBLE.name());
        grugFieldNode3.put("size", 0);
        fieldsArray.add(grugFieldNode3);

        createNode.set("fields", fieldsArray);

        log.info("Creating bucket: {}", BUCKET_NAME);
        String createResponse = sendAndReceive(createNode);
        assertNotNull(createResponse, "Create bucket response should not be null");
        assertFalse(createResponse.contains("Error"), "Create bucket should succeed. Response: " + createResponse);

        // Write to bucket
        ObjectNode writeNode = createActionNode("write");
        ObjectNode fieldValues = MAPPER.createObjectNode();
        fieldValues.put(TEST_FIELD_NAME, TEST_FIELD_VALUE);
        fieldValues.put(TEST_STRING_NAME, TEST_STRING_VALUE);
        fieldValues.put(TIMESTAMP_NAME, TIMESTAMP_VALUE);
        writeNode.set("fieldValues", fieldValues);

        log.info("Writing {} records to bucket: {}", WRITE_AMOUNT, BUCKET_NAME);
        long start = System.currentTimeMillis();
        for (int i = 0; i < WRITE_AMOUNT; i++) {
            String writeResponse = sendAndReceive(writeNode);
            assertNotNull(writeResponse, "Write action response should not be null");
        }
        long end = System.currentTimeMillis();
        log.info("Writing {} records took: {} ms", WRITE_AMOUNT, (end - start));

        start = System.currentTimeMillis();

        // Read from bucket (most recent)
        ObjectNode readNode = createActionNode("read");
        readNode.put("type", ReadActionType.MOST_RECENT.name());

        log.info("Reading most recent record from bucket: {}", BUCKET_NAME);
        String readResponse = sendAndReceive(readNode);
        log.debug("Most recent read response: {}", readResponse);
        assertNotNull(readResponse, "Read action response should not be null");
        assertTrue(readResponse.contains(TEST_STRING_VALUE), "Most recent record should contain: " + TEST_STRING_VALUE + ". Response: " + readResponse);

        // Create stream
        log.info("Creating stream for bucket: {}", BUCKET_NAME);
        ObjectNode streamNode = createActionNode("createStream");
        ArrayNode bucketsNode = MAPPER.createArrayNode();
        bucketsNode.add(BUCKET_NAME);
        streamNode.put("bucketsToStream", bucketsNode);
        String streamResponse = sendAndReceive(streamNode);
        assertNotNull(streamResponse, "Stream response should not be null");
        assertTrue(streamResponse.contains("Stream started"), "Stream creation should succeed. Response: " + streamResponse);

        // Update bucket
        ObjectNode updateNode = createActionNode("write");
        fieldValues = MAPPER.createObjectNode();
        fieldValues.put(TEST_FIELD_NAME, UPDATED_TEST_FIELD_VALUE);
        fieldValues.put(TEST_STRING_NAME, UPDATED_TEST_STRING_VALUE);
        fieldValues.put(TIMESTAMP_NAME, TIMESTAMP_VALUE);
        updateNode.set("fieldValues", fieldValues);
        log.info("Writing one record to update bucket: {}", BUCKET_NAME);
        String updateResponse = sendAndReceive(updateNode);
        assertNotNull(updateResponse, "Update response should not be null");

        byte[] receiveData = new byte[BUFFER_SIZE];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        log.info("Receiving one value from stream for bucket: {}", BUCKET_NAME);
        clientSocket.receive(receivePacket);
        String streamValue = new String(receivePacket.getData(), 0, receivePacket.getLength()).trim();
        log.debug("Received from stream: {}", streamValue);
        assertTrue(streamValue.contains(UPDATED_TEST_STRING_VALUE), "Stream should contain updated value: " + UPDATED_TEST_STRING_VALUE + ". Received: " + streamValue);

        // Read from bucket (full)
        log.info("Reading all records from bucket: {}", BUCKET_NAME);
        readNode.put("type", ReadActionType.FULL.name());
        readResponse = sendAndReceive(readNode);
        //log.debug("Full read response: {}", readResponse);
        assertNotNull(readResponse, "Read action response should not be null");
        assertTrue(readResponse.contains(UPDATED_TEST_STRING_VALUE) || readResponse.contains(TEST_STRING_VALUE),
                "Full read should contain at least one of the written values. Response: " + readResponse);

        end = System.currentTimeMillis();
        log.info("Full read took: {} ms", (end - start));

        // Write to bucket with null
        ObjectNode writeNullNode = createActionNode("write");
        fieldValues = MAPPER.createObjectNode();
        fieldValues.put(TEST_FIELD_NAME, UPDATED_TEST_FIELD_VALUE);
        //fieldValues.put(TEST_STRING_NAME, UPDATED_TEST_STRING_VALUE);
        fieldValues.put(TIMESTAMP_NAME, TIMESTAMP_VALUE);
        writeNullNode.set("fieldValues", fieldValues);
        log.info("Writing one record to update bucket: {}", BUCKET_NAME);
        String writeNullResponse = sendAndReceive(writeNullNode);
        assertNotNull(writeNullResponse, "Update response should not be null");
        assertTrue(writeNullResponse.contains("Error"));
    }

    private String sendAndReceive(DatagramPacket sendPacket, DatagramPacket receivePacket) throws IOException {
        clientSocket.send(sendPacket);
        if (server.isClosed()) {
            log.error("Server is closed, cannot receive response.");
            return null;
        }
        clientSocket.receive(receivePacket);
        return new String(receivePacket.getData(), 0, receivePacket.getLength()).trim();
    }

    @Test
    void testConcurrentWrites() throws IOException, InterruptedException {
        // Create bucket first
        ObjectNode createNode = createActionNode("createBucket");
        ArrayNode fieldsArray = MAPPER.createArrayNode();
        ObjectNode intFieldNode = MAPPER.createObjectNode();
        intFieldNode.put("name", TEST_FIELD_NAME);
        intFieldNode.put("type", FieldType.INT.name());
        intFieldNode.put("size", 0);
        fieldsArray.add(intFieldNode);
        ObjectNode timestampNode = MAPPER.createObjectNode();
        timestampNode.put("name", TIMESTAMP_NAME);
        timestampNode.put("type", FieldType.DOUBLE.name());
        timestampNode.put("size", 0);
        fieldsArray.add(timestampNode);
        createNode.set("fields", fieldsArray);
        String createResponse = sendAndReceive(createNode);
        assertNotNull(createResponse, "Create bucket response should not be null");
        assertFalse(createResponse.contains("Error"), "Create bucket should succeed. Response: " + createResponse);

        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        Object sharedLock = new Object();
        long startTime = System.currentTimeMillis();

        IntStream.range(0, CONCURRENT_WRITE_AMOUNT).forEach(i -> executorService.submit(() -> {
            ObjectNode writeNode = createActionNode("write");
            ObjectNode fieldValues = MAPPER.createObjectNode();
            fieldValues.put(TEST_FIELD_NAME, i); // Use a unique value for each write
            fieldValues.put(TIMESTAMP_NAME, Double.valueOf((double)System.currentTimeMillis())); // Use a unique timestamp
            writeNode.set("fieldValues", fieldValues);
            try {
                String response = sendAndReceive(writeNode);
                assertNotNull(response, "Write response should not be null for message " + i);
                assertFalse(response.contains("Error"), "Write should succeed for message " + i + ". Response: " + response);
            } catch (IOException e) {
                log.error("Error sending write request: {}", e.getMessage());
                fail("Error during concurrent write: " + e.getMessage());
            }
        }));

        executorService.shutdown();
        boolean finished = executorService.awaitTermination(5, TimeUnit.SECONDS); // Adjust timeout as needed
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        assertTrue(finished, "Concurrent writes did not finish within the timeout.");
        log.info("Wrote {} messages using {} threads in {} ms. Throughput: {} messages/s",
                CONCURRENT_WRITE_AMOUNT, NUM_THREADS, totalTime, (double) CONCURRENT_WRITE_AMOUNT / (totalTime / 1000.0));
    }
}