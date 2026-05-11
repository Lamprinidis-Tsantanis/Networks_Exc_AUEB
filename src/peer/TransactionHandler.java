package peer;

import models.Message;
import models.Message.MessageType;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class TransactionHandler implements Runnable {
    private String objectId;
    private double finalPrice;
    private String sellerTransIp;
    private int sellerTransPort;
    private String sharedDir;
    private AuctionClient auctionClient;
    private static final Random RAND = new Random();

    private static final int CONNECTION_TIMEOUT = 10000;
    private static final int SOCKET_TIMEOUT = 5000;

    public TransactionHandler(String sellerIp, int sellerPort, String objectId, double winningBid,
            String sharedDirectory, AuctionClient auctionClient) {
        this.sellerTransIp = sellerIp;
        this.sellerTransPort = sellerPort;
        this.objectId = objectId;
        this.finalPrice = winningBid;
        this.sharedDir = sharedDirectory;
        this.auctionClient = auctionClient;
    }

    @Override
    public void run() {
        System.out.println("[TransactionHandler]> Starting transaction for item: "
                + objectId + " from seller " + sellerTransIp + ":" + sellerTransPort);

        double random = RAND.nextDouble();

        if (random >= 0.7) {
            try {
                System.out.println("[TransactionHandler]> Fetching from server");
                byte[] fileBytes = fetchFileFromServer();
                System.out.println("[TransactionHandler]> Save to Disk");
                saveFileToDisk(fileBytes);
                System.out.println("[TransactionHandler]> Notifying Server");
                notifyServerOfOwnership();
            } catch (TransactionException e) {
                System.err.println("[TransactionHandler]> Transaction failed: " + e.getMessage());
            }
        } else {
            System.out.println("[TransactionHandler]> Bidder decided to cancel the bidding.");
            notifyServerOfCancellation();
        }
    }

    private byte[] fetchFileFromServer() throws TransactionException {
        Socket tcpSocket = null;
        DatagramSocket udpSocket = null;

        try {
            // Create and connect TCP socket with timeout
            tcpSocket = new Socket();
            tcpSocket.connect(new java.net.InetSocketAddress(sellerTransIp, sellerTransPort), CONNECTION_TIMEOUT);
            tcpSocket.setSoTimeout(SOCKET_TIMEOUT);

            // Create UDP socket for file transfer
            udpSocket = new DatagramSocket();
            int myUdpPort = udpSocket.getLocalPort();
            udpSocket.setSoTimeout(5000);

            System.out.println("[TransactionHandler]> Connected to seller via TCP. Local UDP port: " + myUdpPort);

            // Confirm deal and get seller's UDP port
            int sellerUdpPort = confirmDeal(tcpSocket, myUdpPort);

            if (sellerUdpPort == -1) {
                throw new TransactionException("Seller did not provide valid UDP port");
            }

            System.out.println("[TransactionHandler]> Seller will send from UDP port: " + sellerUdpPort);

            // Receive file via UDP
            byte[] file = receiveFileUdp(udpSocket, sellerUdpPort);
            return file;

        } catch (java.net.SocketTimeoutException e) {
            throw new TransactionException(
                    "Timed out connecting to seller at " + sellerTransIp + ":" + sellerTransPort);
        } catch (IOException e) {
            throw new TransactionException("I/O error during seller connection: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new TransactionException("Received an unrecognised object from seller: " + e.getMessage());
        } finally {
            // Clean up TCP socket
            if (tcpSocket != null) {
                try {
                    tcpSocket.close();
                } catch (IOException ignored) {
                }
            }
            // UDP socket will be closed by caller or kept open for transfer
        }
    }

    public int confirmDeal(Socket socket, int myUdpPort)
            throws IOException, TransactionException, ClassNotFoundException {
        // Send transaction request via TCP
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.flush();
        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

        Message confirm = new Message(Message.MessageType.TRANSACTION);
        confirm.put("object_id", objectId);
        confirm.put("winning_bid", String.valueOf(finalPrice));
        confirm.put("buyer_udp_port", myUdpPort);

        System.out.println("[TransactionHandler]> Sending transaction confirmation to seller...");
        out.writeObject(confirm);
        out.flush();
        out.reset();

        // Wait for TCP ACK with seller's UDP port
        Message response = (Message) in.readObject();

        if (response == null) {
            throw new TransactionException("Seller returned null response.");
        }

        if (response.getType() == MessageType.SUCCESS) {
            System.out.println("[TransactionHandler]> Seller confirmed transaction - extracting UDP port");

            // Extract seller's UDP port
            Object portObj = response.get("seller_udp_port");
            if (portObj instanceof Integer) {
                return (Integer) portObj;
            } else if (portObj instanceof String) {
                try {
                    return Integer.parseInt((String) portObj);
                } catch (NumberFormatException e) {
                    throw new TransactionException("Invalid UDP port format from seller.");
                }
            }
        } else {
            String errorMsg = response.getString("message");
            throw new TransactionException("Seller rejected transaction: " + errorMsg);
        }

        return -1;
    }

    private byte[] receiveFileUdp(DatagramSocket mySocket, int sellerPort) throws IOException, TransactionException {
        int expectedSeqId = 0;
        java.util.List<byte[]> fileChunks = new java.util.ArrayList<>();
        boolean lastPacketReceived = false;
        long finalAckSentTime = -1;
        final long FINAL_ACK_TIMER_MS = 3000; // 3 seconds to wait after sending final ACK

        System.out.println("[TransactionHandler]> Starting GBN UDP receiver on port: " + mySocket.getLocalPort());
        mySocket.setSoTimeout(5000);

        java.net.InetAddress sellerAddr = java.net.InetAddress.getByName(sellerTransIp);

        while (true) {
            try {
                // If final ACK was sent and timer expired, exit
                if (lastPacketReceived && System.currentTimeMillis() > finalAckSentTime) {
                    System.out.println("[TransactionHandler]> Final ACK timer expired, exiting");
                    break;
                }

                // Receive packet
                byte[] receiveBuffer = new byte[4096];
                DatagramPacket dgPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                mySocket.receive(dgPacket);

                // Deserialize UDP packet
                models.UdpPacket receivedPkt = deserializeUdpPacket(dgPacket.getData());

                // GBN Logic: Check if packet is in-order
                boolean netIsWorking = networkIsWorking();

                // Handle retransmitted final packet
                if (lastPacketReceived && receivedPkt.getIsFinal()) {
                    if (netIsWorking) {
                        System.out.println("[TransactionHandler]> Received retransmitted FINAL packet, re-ACKing");
                        sendAckToSeller(mySocket, expectedSeqId - 1, sellerAddr, sellerPort);
                    } else {
                        System.out.println(
                                "[TransactionHandler]> Received retransmitted final packet but discarding (simulating loss)");
                    }
                } else if (receivedPkt.getSeqId() == expectedSeqId && netIsWorking) {
                    // ===== IN-ORDER PACKET =====
                    fileChunks.add(receivedPkt.getPayload());
                    System.out.println("[TransactionHandler]> Received packet " + expectedSeqId + " (in-order)");

                    // Check if this is the last packet
                    if (receivedPkt.getIsFinal()) {
                        lastPacketReceived = true;
                        System.out.println("[TransactionHandler]> Received FINAL packet");
                    }

                    expectedSeqId++;

                    // Send cumulative ACK for this in-order packet
                    sendAckToSeller(mySocket, expectedSeqId - 1, sellerAddr, sellerPort);

                    // Start timer when we send the final ACK
                    if (receivedPkt.getIsFinal()) {
                        finalAckSentTime = System.currentTimeMillis() + FINAL_ACK_TIMER_MS;
                        System.out.println("[TransactionHandler]> Final ACK sent, timer started ("
                                + FINAL_ACK_TIMER_MS + "ms)");
                    }

                } else if (netIsWorking) {
                    // out of order packet
                    System.out.println("[TransactionHandler]> Received out-of-order packet "
                            + receivedPkt.getSeqId() + " (expected " + expectedSeqId + "), discarding");

                    // Resend last cumulative ACK (triggers sender's Go-Back-N)
                    sendAckToSeller(mySocket, expectedSeqId - 1, sellerAddr, sellerPort);
                } else if (!netIsWorking) {
                    System.out.println("[TransactionHandler]> Received packet (" + receivedPkt.getSeqId()
                            + ") and discarded it (simulating loss)");
                }

            } catch (java.net.SocketTimeoutException e) {
                // Socket timeout is normal - just loop again
                if (!lastPacketReceived) {
                    throw new TransactionException("Timeout waiting for packet " + expectedSeqId);
                }
                // Otherwise just continue looping until final ACK timer expires
            }
        }

        // Reassemble file from chunks
        byte[] completeFile = reassembleFile(fileChunks);
        System.out.println("[TransactionHandler]> File transfer complete. Total packets: " + fileChunks.size());
        return completeFile;
    }

    private void sendAckToSeller(DatagramSocket mySocket, int seqId, java.net.InetAddress sellerAddr, int sellerPort)
            throws TransactionException {
        if (networkIsWorking()) {
            try {
                // Create ACK packet (cumulative - acks all packets up to seqId)
                models.UdpPacket ackPacket = models.UdpPacket.createAck(seqId);
                byte[] ackData = serializeUdpPacket(ackPacket);

                DatagramPacket dgAck = new DatagramPacket(ackData, ackData.length, sellerAddr, sellerPort);
                mySocket.send(dgAck);

                System.out.println("[TransactionHandler]> Sent cumulative ACK(" + seqId + ")");
            } catch (IOException e) {
                System.err.println("[TransactionHandler]> Failed to send ACK: " + e.getMessage());
            }
        } else {
            System.out.println("[TransactionHandler]> Failed to send ACK(" + seqId + ") (simulated) ");
        }
    }

    private byte[] reassembleFile(java.util.List<byte[]> chunks) throws TransactionException {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            for (byte[] chunk : chunks) {
                if (chunk != null) {
                    baos.write(chunk);
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new TransactionException("Failed to reassemble file: " + e.getMessage());
        }
    }

    private models.UdpPacket deserializeUdpPacket(byte[] data) throws TransactionException {
        try {
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
            return (models.UdpPacket) ois.readObject();
        } catch (Exception e) {
            throw new TransactionException("Failed to deserialize UDP packet: " + e.getMessage());
        }
    }

    private byte[] serializeUdpPacket(models.UdpPacket packet) throws TransactionException {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
            oos.writeObject(packet);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new TransactionException("Failed to serialize UDP packet: " + e.getMessage());
        }
    }

    private void saveFileToDisk(byte[] fileBytes) throws TransactionException {
        try {
            Path dirPath = Paths.get(sharedDir);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }
            Path filePath = dirPath.resolve(objectId + ".txt");
            Files.write(filePath, fileBytes);
            System.out.println("[TransactionHandler]> File saved to: " + filePath.toAbsolutePath());
        } catch (IOException e) {
            throw new TransactionException("Failed to save file: " + e.getMessage());
        }
    }

    private void notifyServerOfOwnership() throws TransactionException {
        Message notify = new Message(MessageType.CONFIRM_OWNERSHIP);
        notify.put("object_id", objectId);
        notify.put("winning_bid", String.valueOf(finalPrice));

        Message response = auctionClient.sendAndReceive(notify);

        if (response == null) {
            throw new TransactionException(
                    "No response from AuctionServer during ownership notification.");
        }

        if (response.getType() != MessageType.SUCCESS) {
            String reason = response.getString("message");
            throw new TransactionException(
                    "AuctionServer rejected ownership notification: "
                            + (reason != null ? reason : "unknown reason"));
        }

        System.out.println("[TransactionHandler]> AuctionServer confirmed new ownership of: " + objectId);
    }

    public void notifyServerOfCancellation() {
        Message cancellation = new Message(MessageType.CANCEL_TRANSACTION);

        cancellation.put("object_id", objectId);

        Message response = auctionClient.sendAndReceive(cancellation);

        if (response != null && response.getType() == MessageType.SUCCESS) {
            System.out.println(
                    "[TransactionHandler]> Server successfully processed the cancellation and penalized reputation.");
        } else {
            String errorMsg = (response != null) ? response.getString("message") : "No response";
            System.err.println("[TransactionHandler]> Failed to notify server of cancellation: " + errorMsg);
        }
    }

    private boolean networkIsWorking() {
        return RAND.nextDouble() < 0.80;
    }

    public static class TransactionException extends Exception {
        public TransactionException(String message) {
            super(message);
        }
    }
}
