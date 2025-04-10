package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.tatuaua.grugtsdb.model.CreateBucketAction;
import org.tatuaua.grugtsdb.model.GrugActionType;
import org.tatuaua.grugtsdb.model.ReadAction;
import org.tatuaua.grugtsdb.model.WriteAction;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class UDPServer {
    private final int port;
    private final int bufferSize;
    private DatagramSocket socket;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public UDPServer(int port, int bufferSize) {
        this.port = port;
        this.bufferSize = bufferSize;
    }

    public void start() {
        try {
            socket = new DatagramSocket(port);
            System.out.println("UDP Server started on port " + port);

            while (true) {
                byte[] buffer = new byte[bufferSize];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String received = new String(packet.getData(), 0, packet.getLength()).trim();
                System.out.println("Received from " + packet.getAddress().getHostAddress() +
                        ":" + packet.getPort() + ": " + received);

                JsonNode rootNode = MAPPER.readTree(received);

                String actionTypeStr = rootNode.get("actionType").asText();

                GrugActionType actionType = GrugActionType.fromString(actionTypeStr); // TODO handle error

                String response;

                switch(actionType) {
                    case CREATE_BUCKET:
                        CreateBucketAction createBucketAction = MAPPER.readValue(received, CreateBucketAction.class);

                        try {
                            DB.createBucket(createBucketAction.getBucketName(), createBucketAction.getFields());
                            response = "Created bucket";
                        } catch (IOException e) {
                            response = "Error";
                        }
                        break;
                    case WRITE:
                        WriteAction writeAction = MAPPER.readValue(received, WriteAction.class);

                        try {
                            DB.writeToBucket(rootNode.get("bucketName").asText(), writeAction.getFieldValues());
                            response = "Wrote to bucket";
                        } catch (IOException e) {
                            response = "Error";
                        }
                        break;
                    case READ:

                        ReadAction readAction = MAPPER.readValue(received, ReadAction.class); // will include more options later
                        response = MAPPER.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(DB.readFullBucket(rootNode.get("bucketName").asText()));
                        break;
                    default:
                        response = "Error";
                        break;
                }

                byte[] responseBytes = response.getBytes();
                DatagramPacket responsePacket = new DatagramPacket(
                        responseBytes,
                        responseBytes.length,
                        packet.getAddress(),
                        packet.getPort()
                );
                socket.send(responsePacket);
            }
        } catch (SocketException e) {
            System.err.println("Failed to create socket: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public void close() {
        if(socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    public boolean isClosed() {
        return socket.isClosed();
    }

    public static void main(String[] args) {
        int port = 12345;
        int bufferSize = 1024;
        UDPServer server = new UDPServer(port, bufferSize);
        server.start();
    }
}