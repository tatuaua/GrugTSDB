package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.tatuaua.grugtsdb.model.CreateBucketAction;
import org.tatuaua.grugtsdb.model.GrugActionType;
import org.tatuaua.grugtsdb.model.WriteAction;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Map;

import static org.tatuaua.grugtsdb.model.GrugActionType.WRITE;

public class UDPServer {
    private final int port;
    private final int bufferSize = 1024;
    private DatagramSocket socket;
    byte[] buffer = new byte[bufferSize];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<GrugActionType, String> responseMessages = Map.of(
            GrugActionType.CREATE_BUCKET, "Created bucket",
            WRITE, "Wrote to bucket",
            GrugActionType.READ, "Read from bucket"
    );

    public UDPServer(int port) {
        this.port = port;
    }

    public void start() {
        try {
            socket = new DatagramSocket(port);
            System.out.println("UDP Server started on port " + port);

            while (true) {
                socket.receive(packet);

                //String received = new String(packet.getData(), 0, packet.getLength()).trim();

                JsonNode rootNode = MAPPER.readTree(packet.getData(), 0, packet.getLength());//MAPPER.readTree(received);

                String actionTypeStr = rootNode.get("actionType").asText();

                GrugActionType actionType = GrugActionType.fromString(actionTypeStr); // TODO handle error

                switch(actionType) {
                    case CREATE_BUCKET:
                        //CreateBucketAction createBucketAction = MAPPER.readValue(received, CreateBucketAction.class);
                        CreateBucketAction createBucketAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), CreateBucketAction.class);
                        try {
                            DB.createBucket(createBucketAction.getBucketName(), createBucketAction.getFields());
                            sendResponse(socket, packet, responseMessages.get(GrugActionType.CREATE_BUCKET));
                        } catch (IOException e) {
                            sendResponse(socket, packet, "Error creating bucket: " + e.getMessage());
                        }
                        break;
                    case WRITE:
                        //WriteAction writeAction = MAPPER.readValue(received, WriteAction.class);
                        WriteAction writeAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), WriteAction.class);
                        try {
                            DB.writeToBucket(rootNode.get("bucketName").asText(), writeAction.getFieldValues());
                            sendResponse(socket, packet, responseMessages.get(WRITE));
                        } catch (IOException e) {
                            System.out.println("Error writing to bucket: " + e.getMessage());
                            sendResponse(socket, packet, "Error writing to bucket: " + e.getMessage());
                        }
                        break;
                    case READ:
                        //ReadAction readAction = MAPPER.readValue(received, ReadAction.class); // will include more options later
                        sendResponse(socket, packet, MAPPER.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(DB.readFromBucket(rootNode.get("bucketName").asText())));
                        break;
                    default:
                        sendResponse(socket, packet, "Unknown action type: " + actionTypeStr);
                        break;
                }
            }
        } catch (SocketException e) {
            System.err.println("Failed to create socket: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void sendResponse(DatagramSocket socket, DatagramPacket packet, String response) throws IOException {
        byte[] responseBytes = response.getBytes();
        DatagramPacket responsePacket = new DatagramPacket(
                responseBytes,
                responseBytes.length,
                packet.getAddress(),
                packet.getPort()
        );
        socket.send(responsePacket);
    }

    public void close() {
        if(socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    public boolean isClosed() {
        return socket.isClosed();
    }
}