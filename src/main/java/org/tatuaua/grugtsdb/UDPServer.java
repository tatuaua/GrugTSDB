package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.tatuaua.grugtsdb.model.*;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class UDPServer {
    private final int port;
    private final int bufferSize = 1024;
    private DatagramSocket socket;
    byte[] buffer = new byte[bufferSize];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<ActionType, String> responseMessages = Map.of(
            ActionType.CREATE_BUCKET, "Bucket created successfully: %s",
            ActionType.WRITE, "Data written to bucket: %s",
            ActionType.CREATE_STREAM, "Stream started for buckets: %s"
    );
    private static final List<Subscriber> subscribers = new ArrayList<>();

    public UDPServer(int port) {
        this.port = port;
    }

    public void start() throws JsonProcessingException {
        try {
            socket = new DatagramSocket(port);
            log.info("UDP Server started on port {}", port);

            while (true) {
                socket.receive(packet);
                JsonNode rootNode = MAPPER.readTree(packet.getData(), 0, packet.getLength());
                String actionTypeStr = rootNode.get("actionType").asText();
                ActionType actionType = ActionType.fromString(actionTypeStr);

                switch (actionType) {
                    case CREATE_BUCKET:
                        CreateBucketAction createBucketAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), CreateBucketAction.class);
                        if (!createBucketAction.hasTimestamp()) {
                            String errorMessage = String.format("Error creating bucket '%s': missing timestamp", createBucketAction.getBucketName());
                            log.error(errorMessage);
                            sendResponse(socket, packet, errorMessage);
                            break;
                        }
                        try {
                            DB.createBucket(createBucketAction.getBucketName(), createBucketAction.getFields());
                            String successMessage = String.format(responseMessages.get(ActionType.CREATE_BUCKET), createBucketAction.getBucketName());
                            sendResponse(socket, packet, successMessage);
                            log.info(successMessage);
                        } catch (IOException e) {
                            String errorMessage = String.format("Error creating bucket '%s': %s", createBucketAction.getBucketName(), e.getMessage());
                            log.error(errorMessage);
                            sendResponse(socket, packet, errorMessage);
                        }
                        break;
                    case WRITE:
                        WriteAction writeAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), WriteAction.class);
                        try {
                            DB.writeToBucket(writeAction.getBucketName(), writeAction.getFieldValues());
                            String successMessage = String.format(responseMessages.get(ActionType.WRITE), writeAction.getBucketName());
                            sendResponse(socket, packet, successMessage);
                            log.debug(successMessage);

                            for (Subscriber s : subscribers) {
                                if (s.bucketsToStream.contains(writeAction.getBucketName())) {
                                    byte[] dataToSend = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(writeAction);
                                    DatagramPacket streamPacket = new DatagramPacket(dataToSend, dataToSend.length, s.address, s.port);
                                    socket.send(streamPacket);
                                    log.debug("Sent update for bucket '{}' to stream subscriber {}:{}", writeAction.getBucketName(), s.address.getHostAddress(), s.port);
                                }
                            }
                        } catch (IOException e) {
                            String errorMessage = String.format("Error writing to bucket '%s': %s", writeAction.getBucketName(), e.getMessage());
                            log.error(errorMessage);
                            sendResponse(socket, packet, errorMessage);
                        }
                        break;
                    case READ:
                        ReadAction readAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), ReadAction.class);
                        String readResult;
                        try {
                            readResult = MAPPER.writerWithDefaultPrettyPrinter()
                                    .writeValueAsString(
                                            switch (readAction.getType()) {
                                                case FULL -> DB.readAll(readAction.getBucketName());
                                                case MOST_RECENT -> DB.readMostRecent(readAction.getBucketName());
                                            }
                                    );
                            sendResponse(socket, packet, readResult);
                            log.debug("Read from bucket '{}' with type '{}'. Response: {}", readAction.getBucketName(), readAction.getType(), readResult);
                        } catch (IOException e) {
                            String errorMessage = String.format("Error reading from bucket '%s': %s", readAction.getBucketName(), e.getMessage());
                            log.error(errorMessage);
                            sendResponse(socket, packet, errorMessage);
                        }
                        break;
                    case CREATE_STREAM:
                        CreateStreamAction createStreamAction = MAPPER.readValue(packet.getData(), 0, packet.getLength(), CreateStreamAction.class);
                        Subscriber newSubscriber = new Subscriber(
                                packet.getAddress(),
                                packet.getPort(),
                                createStreamAction.getBucketsToStream()
                        );
                        subscribers.add(newSubscriber);
                        String streamResponseMessage = String.format(responseMessages.get(ActionType.CREATE_STREAM), createStreamAction.getBucketsToStream());
                        sendResponse(socket, packet, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(streamResponseMessage));
                        log.info("{} for subscriber {}:{}", streamResponseMessage, packet.getAddress().getHostAddress(), packet.getPort());
                        break;
                    default:
                        String unknownActionError = String.format("Unknown action type: %s", actionTypeStr);
                        log.warn(unknownActionError);
                        ErrorResponse error = new ErrorResponse(unknownActionError);
                        sendResponse(socket, packet, MAPPER.writeValueAsString(error));
                        break;
                }
                // Reset the packet buffer for the next receive
                packet.setLength(bufferSize);
            }

        } catch (JsonProcessingException e) {
            String errorMessage = String.format("Failed to parse JSON: %s", e.getMessage());
            log.error(errorMessage, e);
            sendResponse(socket, packet, errorMessage);
        } catch (SocketException e) {
            String errorMessage = String.format("Failed to create or access socket: %s", e.getMessage());
            log.error(errorMessage, e);
            sendResponse(socket, packet, errorMessage);
        } catch (IOException e) {
            String errorMessage = String.format("IO Exception occurred: %s", e.getMessage());
            log.error(errorMessage, e);
            sendResponse(socket, packet, errorMessage);
        } catch (Exception e) {
            String errorMessage = String.format("An unexpected exception occurred: %s", e.getMessage());
            log.error(errorMessage, e);
            sendResponse(socket, packet, errorMessage);
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
                log.info("UDP Server socket closed.");
            }
        }
    }

    public void sendResponse(DatagramSocket socket, DatagramPacket packet, String response) {
        byte[] responseBytes;
        int maxLength = 500;
        if (response.length() > maxLength) {
            String truncatedResponse = response.substring(0, maxLength);
            responseBytes = truncatedResponse.getBytes();
            log.warn("Response to {}:{} was too large and truncated.", packet.getAddress().getHostAddress(), packet.getPort());
        } else {
            responseBytes = response.getBytes();
        }
        DatagramPacket responsePacket = new DatagramPacket(
                responseBytes,
                responseBytes.length,
                packet.getAddress(),
                packet.getPort()
        );

        try {
            socket.send(responsePacket);
            log.debug("Sent response '{}' to {}:{}", response, packet.getAddress().getHostAddress(), packet.getPort());
        } catch (IOException e) {
            log.error("Error sending response to {}:{}: {}", packet.getAddress().getHostAddress(), packet.getPort(), e.getMessage());
        }
    }

    private void removeStreamSub(InetAddress address, int port) {
        boolean removed = subscribers.removeIf(s -> s.address.equals(address) && s.port == port);
        if (removed) {
            log.info("Removed stream subscription for {}:{}", address.getHostAddress(), port);
        } else {
            log.warn("No stream subscription found for {}:{}", address.getHostAddress(), port);
        }
    }

    public boolean isClosed() {
        return socket != null && socket.isClosed();
    }

    private record Subscriber(InetAddress address, int port, List<String> bucketsToStream) {
    }
}