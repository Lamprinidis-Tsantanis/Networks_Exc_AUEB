package peer;

import models.Message;
import models.UdpPacket;

import java.io.*;
import java.net.*;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PeerServer extends Thread {

    private final ExecutorService threadPool;
    private int p2pListeningPort;
    private boolean running = true;
    private ServerSocket serverSocket;
    private String directoryPath;

    public PeerServer() {
        this.threadPool = Executors.newCachedThreadPool();
    }

    public int getListeningPort() {
        return p2pListeningPort;
    }

    public void setDirectory(String dir) {
        this.directoryPath = dir;
    }

    public String getDirectory() {
        return this.directoryPath;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(0);
            this.p2pListeningPort = serverSocket.getLocalPort();
            System.out.println("[PeerServer]> Peer's listening server started on random port: " + p2pListeningPort);

            while (running) {
                Socket incomingSocket = serverSocket.accept();
                threadPool.execute(new PeerConnectionHandler(incomingSocket));
            }

        } catch (IOException e) {
            if (running) {
                System.err.println("[PeerServer]> Server error: " + e.getMessage());
                e.printStackTrace();
            }
        } finally {
            threadPool.shutdown();
        }
    }

    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("[PeerServer]> Failed to close socket: " + e.getMessage());
        }
    }

    private class PeerConnectionHandler implements Runnable {
        private final Socket socket;

        public PeerConnectionHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            ObjectOutputStream out = null;

            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();

                Message request = (Message) in.readObject();
                if (request == null || request.getType() == null)
                    return;

                switch (request.getType()) {

                    case TRANSACTION:
                        System.out.println("[PeerServer]> Received TRANSACTION request from a buyer.");
                        String objId = request.getString("object_id");
                        int buyerUdpPort = ((Number) request.get("buyer_udp_port")).intValue();
                        Path filePath = Paths.get(directoryPath, objId + ".txt");
                        String buyerIp = socket.getInetAddress().getHostAddress();

                        try {
                            // Check if file exists first
                            if (!Files.exists(filePath)) {
                                Message errorMsg = new Message(Message.MessageType.ERROR);
                                errorMsg.put("message", "File not found: " + objId);
                                out.writeObject(errorMsg);
                                out.flush();
                                System.err.println("[PeerServer]> File not found: " + filePath);
                                break;
                            }

                            String fileContent = new String(Files.readAllBytes(filePath));

                            // Create a UDP socket for file transfer
                            try (DatagramSocket udpSocket = new DatagramSocket()) {
                                int sellerUdpPort = udpSocket.getLocalPort();

                                // Send TCP response with seller's UDP port
                                Message successMsg = new Message(Message.MessageType.SUCCESS);
                                successMsg.put("seller_udp_port", sellerUdpPort);
                                successMsg.put("file_size", Files.size(filePath));

                                out.writeObject(successMsg);
                                out.flush();
                                System.out.println("[PeerServer]> Confirmed transaction. Starting UDP transfer on port: " + sellerUdpPort);

                                // Send file via UDP using Go-Back-N
                                sendFileUdp(udpSocket, buyerIp, buyerUdpPort, fileContent.getBytes(), objId);

                                // delete after successful transfer
                                Files.delete(filePath);
                                System.out.println("[PeerServer]> File sent and deleted: " + filePath.getFileName());
                            }

                        } catch (IOException e) {
                            if (out != null) {
                                try {
                                    Message errorMsg = new Message(Message.MessageType.ERROR);
                                    errorMsg.put("message", "Seller could not read or find the file: " + e.getMessage());
                                    out.writeObject(errorMsg);
                                    out.flush();
                                } catch (IOException ignored) {
                                }
                            }
                            System.err.println("[PeerServer]> Failed transaction for " + objId + ": " + e.getMessage());
                        }
                        break;

                    case AUCTION_RESULT:
                        handleAuctionResult(request);
                        break;

                    case CHECK_ACTIVE:
                        // The server is just pinging to see if the socket is open
                        break;

                    default:
                        System.out.println("[PeerServer]> Ignored unknown message type: " + request.getType());
                        break;
                }

            } catch (java.io.EOFException e) {
                // Client closed the connection normally
            }catch (SocketException e){
                // Connection reset/aborted during normal operation - expected after file transfer
                if (e.getMessage() != null && e.getMessage().contains("aborted")) {
                    System.out.println("[PeerServer]> Client connection closed after transaction.");
                } else {
                    System.err.println("[PeerServer]> Socket error: " + e.getMessage());
                }
            }

            catch (Exception e) {
                System.err.println("[PeerServer]> Connection error: " + e.getMessage());
            } finally {
                try {
                    if (out != null) out.close();
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }

        private void handleAuctionResult(Message request) {
            String status = request.getString("status");
            String msg = request.getString("message");
            System.out.println("\n[URGENT NOTIFICATION]> " + status + ": " + msg);

            if (status == null) {
                System.err.println("[PeerServer]> AUCTION_RESULT received with no status field.");
                return;
            }

            switch (status) {
                case "WON":
                    String objectId   = request.getString("object_id");
                    String objectDesc = request.getString("object_description");
                    double finalPrice = Double.parseDouble(request.get("final_price").toString());
                    String sellerIp   = request.getString("p2pIpAddress");
                    int sellerPort    = (Integer) request.get("p2pPort");

                    System.out.println("[PeerServer]> You won item: " + objectId + " (" + objectDesc + ")");
                    System.out.println("[PeerServer]> Final price: " + finalPrice);
                    new Thread(new TransactionHandler(
                            sellerIp, sellerPort, objectId, finalPrice,
                            directoryPath, PeerApp.getAuctionClient()
                    )).start();
                    break;

                case "SOLD":
                    String soldObjectId    = request.getString("object_id");
                    double soldFinalPrice  = Double.parseDouble(request.get("final_price").toString());
                    String buyerUsername   = request.getString("buyer_username");
                    String buyerToken      = request.getString("buyer_token");

                    System.out.println("[PeerServer]> Your item " + soldObjectId + " was sold for "
                            + soldFinalPrice + " to user " + buyerUsername + " (token: " + buyerToken + ")");
                    break;

                case "NO_BIDS":
                    String noBidsObjectId = request.getString("object_id");
                    System.out.println("[PeerServer]> Auction for item " + noBidsObjectId
                            + " ended with no bids. Item was not sold.");
                    break;

                case "AUCTION_CANCELLED":
                    System.out.println("[PeerServer]> The auction was cancelled because the seller disconnected.");
                    break;

                case "NEW_HIGHEST_BID":
                    String bidObjectId = request.getString("object_id");
                    Object newBid = request.get("current_bid");
                    System.out.println("[PeerServer]> New highest bid on item " + bidObjectId + ": " + newBid);
                    break;

                default:
                    System.out.println("[PeerServer]> Unhandled AUCTION_RESULT status: " + status);
                    break;
            }
        }

        /**
         * Sends a file via UDP using Go-Back-N protocol
         */
        private void sendFileUdp(DatagramSocket socket, String buyerIp, int buyerPort, byte[] fileData, String objId) throws IOException {
            final int PACKET_SIZE = 4000;  // Leave room for headers
            final int WINDOW_SIZE = 5;
            final int TIMEOUT_MS = 2000;

            InetAddress buyerAddr = InetAddress.getByName(buyerIp);

            // Split file into chunks
            List<byte[]> chunks = splitFileIntoChunks(fileData, PACKET_SIZE);
            int totalPackets = chunks.size();

            System.out.println("[PeerServer]> Sending " + totalPackets + " packets to buyer via UDP");
            socket.setSoTimeout(TIMEOUT_MS);

            int base = 0;
            int nextSeqNum = 0;

            while (base < totalPackets) {
                // Send packets within window
                while (nextSeqNum < base + WINDOW_SIZE && nextSeqNum < totalPackets) {
                    boolean isFinal = (nextSeqNum == totalPackets - 1);
                    UdpPacket packet = new UdpPacket(nextSeqNum, chunks.get(nextSeqNum), false, isFinal);
                    byte[] packetData = serializeUdpPacket(packet);

                    DatagramPacket dgPacket = new DatagramPacket(packetData, packetData.length, buyerAddr, buyerPort);
                    socket.send(dgPacket);

                    System.out.println("[PeerServer]> Sent packet " + nextSeqNum + (isFinal ? " (FINAL)" : ""));
                    nextSeqNum++;
                }

                // Wait for ACK
                try {
                    byte[] ackBuffer = new byte[1024];
                    DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
                    socket.receive(ackPacket);

                    UdpPacket receivedAck = deserializeUdpPacket(ackPacket.getData());
                    if (receivedAck.getIsAck()) {
                        int ackNum = receivedAck.getSeqId();
                        System.out.println("[PeerServer]> Received ACK " + ackNum);

                        // Cumulative ACK - advance window
                        if (ackNum >= base) {
                            base = ackNum + 1;
                        }
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println("[PeerServer]> Timeout - resending from packet " + base);
                    nextSeqNum = base;  // Go-Back-N: reset to base
                }
            }

            System.out.println("[PeerServer]> File transfer complete for: " + objId);
        }

        /**
         * Splits file data into chunks for UDP transmission
         */
        private List<byte[]> splitFileIntoChunks(byte[] fileData, int chunkSize) {
            List<byte[]> chunks = new ArrayList<>();
            int offset = 0;

            while (offset < fileData.length) {
                int length = Math.min(chunkSize, fileData.length - offset);
                byte[] chunk = new byte[length];
                System.arraycopy(fileData, offset, chunk, 0, length);
                chunks.add(chunk);
                offset += length;
            }

            return chunks;
        }

        /**
         * Deserializes a UDP packet from bytes
         */
        private UdpPacket deserializeUdpPacket(byte[] data) throws IOException {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                ObjectInputStream ois = new ObjectInputStream(bais);
                return (UdpPacket) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize UDP packet", e);
            }
        }

        /**
         * Serializes a UDP packet to bytes
         */
        private byte[] serializeUdpPacket(UdpPacket packet) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(packet);
            oos.flush();
            return baos.toByteArray();
        }
    }
}
