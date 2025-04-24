package org.tatuaua.grugtsdb.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.tatuaua.grugtsdb.engine.Engine;
import org.tatuaua.grugtsdb.server.model.ActionType;
import org.tatuaua.grugtsdb.server.model.AggregateReadAction;
import org.tatuaua.grugtsdb.server.model.CreateBucketAction;
import org.tatuaua.grugtsdb.server.model.CreateStreamAction;
import org.tatuaua.grugtsdb.server.model.ErrorResponse;
import org.tatuaua.grugtsdb.server.model.ReadAction;
import org.tatuaua.grugtsdb.server.model.WriteAction;

@Slf4j
public class Server {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<Subscriber> SUBSCRIBERS = new ArrayList<>();

    private final int port;
    private final int bufferSize = 1024;
    private DatagramSocket socket;
    private final byte[] buffer = new byte[bufferSize];
    private final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

    public Server(int port) {
        this.port = port;
    }

    public static void main(String[] args) {
        Server server = new Server(8080);
        server.start();
    }

    public void stop() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
            log.info("UDP Server socket closed.");
        }
    }

    public void start() {
        try {
            socket = new DatagramSocket(port);
            log.info("UDP Server started on port {}", port);

            while (true) {
                receiveAndProcessPacket();
            }

        } catch (SocketException e) {
            log.error("Failed to create or access socket on port {}: {}", port, e.getMessage(), e);
            sendErrorResponse(packet, "Failed to create or access socket.");
        } catch (IOException e) {
            log.error("IO Exception occurred during server operation: {}", e.getMessage(), e);
            sendErrorResponse(packet, "IO error during server operation.");
        } catch (Exception e) {
            log.error("An unexpected exception occurred: {}", e.getMessage(), e);
            sendErrorResponse(packet, "An unexpected error occurred.");
        } finally {
            stop(); // Ensure socket is closed on exit
        }
    }

    private void receiveAndProcessPacket() throws IOException {
        socket.receive(packet);
        try {
            JsonNode rootNode = MAPPER.readTree(packet.getData(), 0, packet.getLength());
            String actionTypeStr = rootNode.get("actionType").asText();
            ActionType actionType = ActionType.fromString(actionTypeStr);

            switch (actionType) {
                case CREATE_BUCKET -> handleCreateBucket(packet, rootNode);
                case WRITE -> handleWrite(packet, rootNode);
                case READ -> handleRead(packet, rootNode);
                case AGGREGATE_READ -> handleAggregateRead(packet, rootNode);
                case CREATE_STREAM -> handleCreateStream(packet, rootNode);
                default -> handleUnknownAction(packet, actionType.toString());
            }
        } catch (JsonProcessingException e) {
            log.warn("Received invalid JSON from {}:{}: {}", packet.getAddress().getHostAddress(), packet.getPort(), e.getMessage());
            sendErrorResponse(packet, "Invalid JSON received.");
        } finally {
            // Reset the packet buffer for the next receive
            packet.setLength(bufferSize);
        }
    }

    private void handleCreateBucket(DatagramPacket packet, JsonNode rootNode) throws IOException {
        try {
            CreateBucketAction createBucketAction = MAPPER.treeToValue(rootNode, CreateBucketAction.class);
            if (!createBucketAction.hasTimestamp()) {
                String errorMessage = String.format("Error creating bucket '%s': missing timestamp", createBucketAction.getBucketName());
                log.error(errorMessage);
                sendResponse(packet, errorMessage);
                return;
            }
            Engine.createBucket(createBucketAction.getBucketName(), createBucketAction.getFields());
            String successMessage = ActionType.getResponseMessage(ActionType.CREATE_BUCKET, createBucketAction.getBucketName());
            sendResponse(packet, successMessage);
            log.info(successMessage);
        } catch (IOException e) {
            String errorMessage = String.format("Error creating bucket: %s", e.getMessage());
            log.error(errorMessage);
            sendResponse(packet, errorMessage);
        }
    }

    private void handleWrite(DatagramPacket packet, JsonNode rootNode) throws IOException {
        try {
            WriteAction writeAction = MAPPER.treeToValue(rootNode, WriteAction.class);
            if (!writeAction.hasValidTimestamp()) {
                String errorMessage = String.format("Error writing to bucket '%s': invalid timestamp", writeAction.getBucketName());
                log.error(errorMessage);
                sendResponse(packet, errorMessage);
                return;
            }
            Engine.writeToBucket(writeAction.getBucketName(), writeAction.getFieldValues());
            String successMessage = ActionType.getResponseMessage(ActionType.WRITE, writeAction.getBucketName());
            sendResponse(packet, successMessage);
            log.info(successMessage);
            notifySubscribers(writeAction);
        } catch (IOException e) {
            String errorMessage = String.format("Error writing to bucket: %s", e.getMessage());
            log.error(errorMessage);
            sendResponse(packet, errorMessage);
        }
    }

    private void handleRead(DatagramPacket packet, JsonNode rootNode) throws IOException {
        try {
            ReadAction readAction = MAPPER.treeToValue(rootNode, ReadAction.class);
            String readResult = MAPPER.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(
                            switch (readAction.getType()) {
                                case FULL -> Engine.readAll(readAction.getBucketName());
                                case MOST_RECENT -> Engine.readMostRecent(readAction.getBucketName());
                            }
                    );
            sendResponse(packet, readResult);
            log.info("Read from bucket '{}' with type '{}'. Response: {}", readAction.getBucketName(), readAction.getType(), readResult);
        } catch (IOException e) {
            String errorMessage = String.format("Error reading from bucket: %s", e.getMessage());
            log.error(errorMessage);
            sendResponse(packet, errorMessage);
        }
    }

    private void handleAggregateRead(DatagramPacket packet, JsonNode rootNode) throws IOException {
        try {
            AggregateReadAction aggregateReadAction = MAPPER.treeToValue(rootNode, AggregateReadAction.class);
            String readResult = MAPPER.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(
                        Engine.aggregateRead(
                            aggregateReadAction.getBucketName(),
                            aggregateReadAction.getTimeRangeStart(),
                            aggregateReadAction.getTimeRangeEnd(),
                            aggregateReadAction.getFieldName(),
                            aggregateReadAction.getAggregationType()
                        )
                    );
            sendResponse(packet, readResult);
            log.info("Aggregate read from bucket '{}' with type '{}'. Response: {}", aggregateReadAction.getBucketName(), aggregateReadAction.getAggregationType(), readResult);
        } catch (IOException e) {
            String errorMessage = String.format("Error performing aggregate read: %s", e.getMessage());
            log.error(errorMessage);
            sendResponse(packet, errorMessage);
        }
    }

    private void handleCreateStream(DatagramPacket packet, JsonNode rootNode) throws IOException {
        try {
            CreateStreamAction createStreamAction = MAPPER.treeToValue(rootNode, CreateStreamAction.class);
            Subscriber newSubscriber = new Subscriber(
                    packet.getAddress(),
                    packet.getPort(),
                    createStreamAction.getBucketsToStream()
            );
            SUBSCRIBERS.add(newSubscriber);
            String streamResponseMessage = ActionType.getResponseMessage(ActionType.CREATE_STREAM, createStreamAction.getBucketsToStream().toString());
            sendResponse(packet, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(streamResponseMessage));
            log.info("{} for subscriber {}:{}", streamResponseMessage, packet.getAddress().getHostAddress(), packet.getPort());
        } catch (IOException e) {
            log.error("Error creating stream subscription: {}", e.getMessage());
            sendResponse(packet, "Error creating stream subscription.");
        }
    }

    private void handleUnknownAction(DatagramPacket packet, String actionTypeStr) throws IOException {
        String errorMessage = String.format("Unknown action type: %s", actionTypeStr);
        log.warn(errorMessage);
        sendErrorResponse(packet, errorMessage);
    }

    private void notifySubscribers(WriteAction writeAction) throws IOException {
        byte[] dataToSend = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(writeAction);
        for (Subscriber subscriber : SUBSCRIBERS) {
            if (subscriber.bucketsToStream().contains(writeAction.getBucketName())) {
                DatagramPacket streamPacket = new DatagramPacket(dataToSend, dataToSend.length, subscriber.address(), subscriber.port());
                socket.send(streamPacket);
                log.info("Sent update for bucket '{}' to stream subscriber {}:{}", writeAction.getBucketName(), subscriber.address().getHostAddress(), subscriber.port());
            }
        }
    }

    public void sendResponse(DatagramPacket packet, String response) {
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
            log.info("Sent response '{}' to {}:{}", response, packet.getAddress().getHostAddress(), packet.getPort());
        } catch (IOException e) {
            log.error("Error sending response to {}:{}: {}", packet.getAddress().getHostAddress(), packet.getPort(), e.getMessage());
        }
    }

    private void sendErrorResponse(DatagramPacket packet, String errorMessage) {
        try {
            ErrorResponse errorResponse = new ErrorResponse(errorMessage);
            String jsonError = MAPPER.writeValueAsString(errorResponse);
            sendResponse(packet, jsonError);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize error response: {}", e.getMessage());
            sendResponse(packet, "Internal server error.");
        }
    }

    private void removeStreamSub(InetAddress address, int port) {
        boolean removed = SUBSCRIBERS.removeIf(s -> s.address().equals(address) && s.port() == port);
        if (removed) {
            log.info("Removed stream subscription for {}:{}", address.getHostAddress(), port);
        } else {
            log.warn("No stream subscription found for {}:{}", address.getHostAddress(), port);
        }
    }

    public boolean isClosed() {
        return socket != null && socket.isClosed();
    }

    private record Subscriber(InetAddress address, int port, List<String> bucketsToStream) {}
}