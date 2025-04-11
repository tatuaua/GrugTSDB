package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tatuaua.grugtsdb.model.GrugFieldType;

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
        executor.submit(() -> server.start());

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
        // Create bucket
        ObjectNode createNode = createActionNode("createBucket", "testBucket");
        ArrayNode fieldsArray = MAPPER.createArrayNode();
        ObjectNode grugFieldNode = MAPPER.createObjectNode();
        grugFieldNode.put("name", "testField");
        grugFieldNode.put("type", GrugFieldType.INT.name());
        grugFieldNode.put("size", 0);
        fieldsArray.add(grugFieldNode);

        ObjectNode grugFieldNode2 = MAPPER.createObjectNode();
        grugFieldNode2.put("name", "testString");
        grugFieldNode2.put("type", GrugFieldType.STRING.name());
        grugFieldNode2.put("size", 50);
        fieldsArray.add(grugFieldNode2);
        createNode.set("fields", fieldsArray);

        String createResponse = sendAndReceive(createNode);
        assertNotNull(createResponse, "Create bucket response should not be null");

        // Write to bucket
        ObjectNode writeNode = createActionNode("write", "testBucket");
        ObjectNode fieldValues = MAPPER.createObjectNode();
        fieldValues.put("testField", 42);
        fieldValues.put("testString", "balls");
        writeNode.set("fieldValues", fieldValues);

        long start = System.currentTimeMillis();
        for(int i = 0; i < 10000; i++) {
            String writeResponse = sendAndReceive(writeNode);
            assertNotNull(writeResponse, "Write action response should not be null");
        }
        long end = System.currentTimeMillis();
        System.out.println("Upload took: " + (end-start));

        start = System.currentTimeMillis();
        // Read from bucket
        ObjectNode readNode = createActionNode("read", "testBucket");

        String readResponse = sendAndReceive(readNode);
        assertNotNull(readResponse, "Read action response should not be null");

        end = System.currentTimeMillis();
        System.out.println("Read took: " + (end-start));
    }
}