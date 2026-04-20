package peer;

import models.Message;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
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
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                Message request = (Message) in.readObject();
                if (request == null || request.getType() == null)
                    return;

                switch (request.getType()) {

                    case TRANSACTION:
                        System.out.println("[PeerServer]> Received TRANSACTION request from a buyer.");
                        String objId = request.getString("object_id");
                        Path filePath = Paths.get(directoryPath, objId + ".txt");

                        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
                            try {
                                String fileContent = new String(Files.readAllBytes(filePath));

                                Message successMsg = new Message(Message.MessageType.SUCCESS);
                                successMsg.put("file_data", fileContent);

                                out.writeObject(successMsg);
                                out.flush();
                                System.out.println("[PeerServer]> Sent file contents for " + objId + " to buyer.");

                                Files.delete(filePath);
                                System.out.println("[PeerServer]> Deleted local file: " + filePath.getFileName());

                            } catch (IOException e) {
                                Message errorMsg = new Message(Message.MessageType.ERROR);
                                errorMsg.put("message", "Seller could not read or find the file.");
                                out.writeObject(errorMsg);
                                out.flush();
                                System.err.println("[PeerServer]> Failed transaction for " + objId + ": " + e.getMessage());
                            }
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
            } catch (Exception e) {
                System.err.println("[PeerServer]> Connection error: " + e.getMessage());
            } finally {
                try {
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
    }
}
