package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

                String response = "";

                JsonNode rootNode = MAPPER.readTree(received);

                if(rootNode.get("actionType").asText().equals("write")) {
                    WriteAction action = MAPPER.readValue(received, WriteAction.class);

                    try {
                        IO.writeToBucket(rootNode.get("bucketName").asText(), action.getFieldValues());
                    } catch (IOException e) {
                        response = "Error";
                    }
                } else if(rootNode.get("actionType").asText().equals("createBucket")) {

                }

                byte[] responseBytes = response.getBytes();
                DatagramPacket responsePacket = new DatagramPacket(
                        responseBytes,
                        responseBytes.length,
                        packet.getAddress(),
                        packet.getPort()
                );
                socket.send(responsePacket);

                System.out.println("Sent: " + response);
            }
        } catch (SocketException e) {
            System.err.println("Failed to create socket: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
            System.out.println("Server stopped");
        }
    }

    public static void main(String[] args) {
        int port = 12345;
        int bufferSize = 1024;
        UDPServer server = new UDPServer(port, bufferSize);
        server.start();
    }
}