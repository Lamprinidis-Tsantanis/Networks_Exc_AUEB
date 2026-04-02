package peer;

import models.Message;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * It extends Thread so that it can run continuously in the background.
 * Needs to read the keyboard input and needs to wait silently for message from the network
 */
public class PeerServer extends Thread {

    private final ExecutorService threadPool;
    private int listeningPort;
    private boolean running = true;
    private ServerSocket serverSocket;

    public PeerServer() {
        this.threadPool = Executors.newCachedThreadPool();
    }

    public int getListeningPort() {
        return listeningPort;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(0); // 0 gives us one random available port
            this.listeningPort = serverSocket.getLocalPort();
            System.out.println("[PeerServer]> Peer's listening server started on random port: " + listeningPort);

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
                        // T-25: A winning buyer has connected to us to claim their file.
                        System.out.println("[PeerServer]> Received TRANSACTION request from a buyer.");
                        // TODO: Implement Transaction logic (Read object_id, send file, delete local
                        // copy)
                        break;

                    case AUCTION_RESULT:
                        String status = request.getString("status");
                        String msg = request.getString("message");
                        System.out.println("\n[URGENT NOTIFICATION]> " + status + ": " + msg);
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
