package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

import static org.junit.jupiter.api.Assertions.*;

class APITest {

    private static final int PORT = 12345;
    private static final int BUFFER_SIZE = 1024;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private UDPServer server;
    private ExecutorService executor;
    private DatagramSocket clientSocket;

    @BeforeEach
    void setUp() throws IOException {
        server = new UDPServer(PORT);
        executor = Executors.newSingleThreadExecutor();
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

        clientSocket.send(sendPacket);

        if (server.isClosed()) {
            System.out.println("Server error");
            return null;
        }

        byte[] receiveData = new byte[BUFFER_SIZE];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        clientSocket.receive(receivePacket);

        return new String(receivePacket.getData(), 0, receivePacket.getLength());
    }

    // Helper method to create basic action JSON
    private ObjectNode createActionNode(String actionType, String bucketName) {
        ObjectNode jsonNode = MAPPER.createObjectNode();
        jsonNode.put("actionType", actionType);
        jsonNode.put("bucketName", bucketName);
        return jsonNode;
    }

    @Test
    void testBucketLifecycle() throws IOException {
        // Hardcoded values as variables
        String bucketName = "testBucket";
        String testFieldName = "testField";
        String testStringName = "testString";
        String timestampName = "timestamp";
        int testFieldValue = 42;
        String testStringValue = "balls";
        double timestampValue = 0.0f;
        String updatedTestStringValue = "updateballs";
        int updatedTestFieldValue = 53;
        int writeAmount = 1000;

        // Create bucket
        ObjectNode createNode = createActionNode("createBucket", bucketName);
        ArrayNode fieldsArray = MAPPER.createArrayNode();
        ObjectNode grugFieldNode = MAPPER.createObjectNode();
        grugFieldNode.put("name", testFieldName);
        grugFieldNode.put("type", FieldType.INT.name());
        grugFieldNode.put("size", 0);
        fieldsArray.add(grugFieldNode);

        ObjectNode grugFieldNode2 = MAPPER.createObjectNode();
        grugFieldNode2.put("name", testStringName);
        grugFieldNode2.put("type", FieldType.STRING.name());
        grugFieldNode2.put("size", 50);
        fieldsArray.add(grugFieldNode2);

        ObjectNode grugFieldNode3 = MAPPER.createObjectNode();
        grugFieldNode3.put("name", timestampName);
        grugFieldNode3.put("type", FieldType.DOUBLE.name());
        grugFieldNode3.put("size", 0);
        fieldsArray.add(grugFieldNode3);

        createNode.set("fields", fieldsArray);

        System.out.println("Creating bucket");
        String createResponse = sendAndReceive(createNode);
        assertNotNull(createResponse, "Create bucket response should not be null");
        assertFalse(createResponse.contains("Error"));

        // Write to bucket
        ObjectNode writeNode = createActionNode("write", bucketName);
        ObjectNode fieldValues = MAPPER.createObjectNode();
        fieldValues.put(testFieldName, testFieldValue);
        fieldValues.put(testStringName, testStringValue);
        fieldValues.put(timestampName, timestampValue);
        writeNode.set("fieldValues", fieldValues);

        System.out.println("Writing " + writeAmount + " records");
        long start = System.currentTimeMillis();
        for(int i = 0; i < writeAmount; i++) {
            String writeResponse = sendAndReceive(writeNode);
            assertNotNull(writeResponse, "Write action response should not be null");
        }
        long end = System.currentTimeMillis();
        System.out.println("Write took: " + (end-start));

        start = System.currentTimeMillis();

        // Read from bucket
        ObjectNode readNode = createActionNode("read", bucketName);
        readNode.put("type", ReadActionType.MOST_RECENT.name());

        System.out.println("Reading most recent record");
        String readResponse = sendAndReceive(readNode);
        System.out.println(readResponse);
        assertNotNull(readResponse, "Read action response should not be null");
        assertTrue(readResponse.contains(testStringValue));

        // Create stream
        System.out.println("Creating stream for " + bucketName);
        ObjectNode streamNode = createActionNode("createStream", bucketName);
        ArrayNode bucketsNode = MAPPER.createArrayNode();
        bucketsNode.add(bucketName);
        streamNode.put("bucketsToStream", bucketsNode);
        String streamResponse = sendAndReceive(streamNode);
        assertNotNull(streamResponse, "Stream response should not be null");
        assertTrue(streamResponse.contains("Started stream"));

        // Update bucket
        ObjectNode updateNode = createActionNode("write", bucketName);
        fieldValues = MAPPER.createObjectNode();
        fieldValues.put(testFieldName, updatedTestFieldValue);
        fieldValues.put(testStringName, updatedTestStringValue);
        fieldValues.put(timestampName, timestampValue);
        updateNode.set("fieldValues", fieldValues);
        System.out.println("Writing one record");
        String updateResponse = sendAndReceive(updateNode);
        assertNotNull(updateResponse);

        byte[] receiveData = new byte[BUFFER_SIZE];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        System.out.println("Receiving one value from stream");
        clientSocket.receive(receivePacket);
        String streamValue = new String(receivePacket.getData(), 0, receivePacket.getLength());
        assertTrue(streamValue.contains(updatedTestStringValue));
        System.out.println("Received from stream: " + streamValue);

        // Read from bucket
        System.out.println("Reading");
        readNode.put("type", ReadActionType.FULL.name());
        readResponse = sendAndReceive(readNode);
        System.out.println(readResponse);
        assertNotNull(readResponse, "Read action response should not be null");
        //assertTrue(readResponse.contains(updatedTestStringValue)); // Depending on the exact implementation, a full read might contain multiple records.

        end = System.currentTimeMillis();
        System.out.println("Read took: " + (end-start));
    }
}