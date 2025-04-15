package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.tatuaua.grugtsdb.model.*;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UDPServer {
    private final int port;
    private final int bufferSize = 1024;
    private DatagramSocket socket;
    byte[] buffer = new byte[bufferSize];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<ActionType, String> responseMessages = Map.of(
            ActionType.CREATE_BUCKET, "Created bucket",
            ActionType.WRITE, "Wrote to bucket",
            ActionType.STREAM, "Started stream"
    );
    private static final List<Subscriber> subscribers = new ArrayList<>();

    public UDPServer(int port) {
        this.port = port;
    }

    public void start() throws JsonProcessingException {
        try {
            socket = new DatagramSocket(port);
            System.out.println("UDP Server started on port " + port);

            while (true) {
                socket.receive(packet);
                JsonNode rootNode = MAPPER.readTree(packet.getData(), 0, packet.getLength());//MAPPER.readTree(received);
                String actionTypeStr = rootNode.get("actionType").asText();
                ActionType actionType = ActionType.fromString(actionTypeStr); // TODO handle error

                switch (actionType) {
                    case CREATE_BUCKET:
                        CreateBucketAction createBucketAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), CreateBucketAction.class);
                        try {
                            DB.createBucket(createBucketAction.getBucketName(), createBucketAction.getFields());
                            sendResponse(socket, packet, responseMessages.get(ActionType.CREATE_BUCKET));
                        } catch (IOException e) {
                            sendResponse(socket, packet, "Error creating bucket: " + e.getMessage());
                        }
                        break;
                    case WRITE:
                        WriteAction writeAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), WriteAction.class);
                        try {
                            DB.writeToBucket(writeAction.getBucketName(), writeAction.getFieldValues());
                            sendResponse(socket, packet, responseMessages.get(ActionType.WRITE));
                        } catch (IOException e) {
                            System.out.println("Error writing to bucket: " + e.getMessage());
                            sendResponse(socket, packet, "Error writing to bucket: " + e.getMessage());
                        }
                        break;
                    case READ:
                        ReadAction readAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), ReadAction.class);
                        sendResponse(socket, packet, MAPPER.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(
                                        switch (readAction.getType()) {
                                            case FULL -> DB.readAll(readAction.getBucketName());
                                            case MOST_RECENT -> DB.readMostRecent(readAction.getBucketName());
                                        }
                                )
                        );
                        break;
                    case STREAM:
                        StreamAction streamAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), StreamAction.class);

                    default:
                        ErrorResponse error = new ErrorResponse("Unknown action type: " + actionTypeStr);
                        sendResponse(socket, packet, MAPPER.writeValueAsString(error));
                        break;
                }
            }

        } catch (JsonProcessingException e) {
            System.err.println("Failed to parse JSON: " + e.getMessage());
            e.printStackTrace();
            sendResponse(socket, packet, MAPPER.writeValueAsString(new ErrorResponse("Failed to parse JSON: " + e.getMessage())));
        } catch (SocketException e) {
            System.err.println("Failed to create or access socket: " + e.getMessage());
            e.printStackTrace();
            sendResponse(socket, packet, MAPPER.writeValueAsString(new ErrorResponse("Failed to create or access socket: " + e.getMessage())));
        } catch (IOException e) {
            System.err.println("IO Exception: " + e.getMessage());
            e.printStackTrace();
            sendResponse(socket, packet, MAPPER.writeValueAsString(new ErrorResponse("IO Exception: " + e.getMessage())));
        } catch (Exception e) {
            e.printStackTrace();
            sendResponse(socket, packet, MAPPER.writeValueAsString(new ErrorResponse("Exception: " + e.getMessage())));
        }
    }

    public void sendResponse(DatagramSocket socket, DatagramPacket packet, String response) {
        byte[] responseBytes = response.getBytes();
        if(responseBytes.length > 500) responseBytes = "too big".getBytes();
        DatagramPacket responsePacket = new DatagramPacket(
                responseBytes,
                responseBytes.length,
                packet.getAddress(),
                packet.getPort()
        );

        try {
            socket.send(responsePacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeStreamSub(InetAddress address, int port) {
        for(int i = 0; i < subscribers.size(); i++) {
            Subscriber s = subscribers.get(i);
            if(s.address.equals(address) && s.port == port) {
                subscribers.remove(i);
                break;
            }
        }
    }

    public boolean isClosed() {
        return socket.isClosed();
    }

    private record Subscriber(InetAddress address, int port, List<String> bucketsToStream) {}
}