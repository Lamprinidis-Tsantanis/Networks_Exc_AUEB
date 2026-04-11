package peer;

import models.Message;
import models.Message.MessageType;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.BufferedReader;
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

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(0); // 0 gives us one random available port
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

    /*
     * Inner class to handle incoming connections from the AuctionServer or other
     * Peers.
     */
    private class PeerConnectionHandler implements Runnable {
        private final Socket socket;

        public PeerConnectionHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

                Message request = (Message) in.readObject();
                if (request == null)
                    return;

                switch (request.getType()) {

                    case CHECK_ACTIVE:
                        /*
                         * The central AuctionServer is pinging us to see if we are still alive.
                         * We don't necessarily need to reply. The fact that the socket connected
                         * and didn't throw an IOException means we are alive.
                         */
                        System.out.println("[PeerServer]> Received CHECK_ACTIVE ping from Server.");
                        break;

                    case TRANSACTION:
                        System.out.println("[PeerServer]> Received TRANSACTION request from a buyer.");
                        String objId = request.getString("object_id");

                        Path filePath = Paths.get(directoryPath, objId + ".txt");

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
                        break;

                    case AUCTION_RESULT:
                        String status = request.getString("status");
                        String msg = request.getString("message");
                        System.out.println("\n[URGENT NOTIFICATION]> " + status + ": " + msg);

                        String objectId   = request.getString("object_id");
                        String objectDesc = request.getString("object_description");
                        double finalPrice = Double.parseDouble(request.getString("final_price"));
                        int    timestamp = Integer.parseInt(request.getString("timestamp"));

                        switch (status) {
                            case "WON":
                                objectId   = request.getString("object_id");
                                objectDesc = request.getString("object_description");
                                finalPrice = Double.parseDouble(request.getString("final_price"));
                                String sellerTransIp = request.getString("seller_trans_ip");
                                String sellerTransPort = request.getString("seller_trans_port");

                            case "SOLD":
                                objectId   = request.getString("object_id");
                                objectDesc = request.getString("object_description");
                                finalPrice = Double.parseDouble(request.getString("final_price"));
                                String buyerUsername = request.getString("buyer_username");
                                int    buyerToken   = Integer.parseInt(request.getString("buyer_token"));



                        }
                        break;

                    default:
                        System.out.println("[PeerServer]> Ignored unknown message type: " + request.getType());
                }

            } catch (Exception e) {
                System.err.println("[PeerServer]> Connection error: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

}
