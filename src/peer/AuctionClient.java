package peer;

import models.Message;
import utils.Constants;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * AuctionClient handles all outgoing communication from the Peer to the central
 * AuctionServer.
 * It establishes a persistent connection, manages serialization streams,
 * and automatically injects the session token into every request once logged
 * in.
 */
public class AuctionClient {

    private Socket requestSocket;
    private ObjectOutputStream writer;
    private ObjectInputStream reader;
    private String tokenID = null;
    private boolean isPolling = false;
    private Thread pollingThread;

    /**
     * Connects to the AuctionServer using the IP and Port defined in
     * Constants.java.
     * Initializes the output and input streams for object serialization.
     *
     * @return {@code true} if connection was successful, {@code false} otherwise.
     */
    public boolean connect() {
        try {
            requestSocket = new Socket(Constants.SERVER_IP, Constants.SERVER_PORT);

            writer = new ObjectOutputStream(requestSocket.getOutputStream());
            writer.flush();
            reader = new ObjectInputStream(requestSocket.getInputStream());

            System.out.println("[AuctionClient]> Successfully connected to AuctionServer at "
                    + Constants.SERVER_IP + ":" + Constants.SERVER_PORT);
            return true;

        } catch (UnknownHostException unknownHost) {
            System.err.println("[AuctionClient]> Unknown host: " + Constants.SERVER_IP);
            return false;
        } catch (IOException ioException) {
            System.err.println("[AuctionClient]> I/O Error during connection: " + ioException.getMessage());
            return false;
        }
    }

    /**
     * Sends a Message object to the server.
     * Automatically includes the token_id in the payload if the user is currently
     * logged in.
     *
     * @param msg The Message object to be sent.
     */
    public void sendMessage(Message msg) {
        try {
            if (tokenID != null) {
                msg.put("token", tokenID);
            }

            writer.writeObject(msg);
            writer.flush();
            writer.reset(); // Clear the cache so updated objects aren't sent as stale references

        } catch (IOException e) {
            System.err.println("[AuctionClient]> Error sending message: " + e.getMessage());
        }
    }

    /**
     * Blocks and waits to receive a Message object from the server.
     *
     * @return The received Message object, or {@code null} if the connection
     *         dropped or failed.
     */
    public Message receiveMessage() {
        try {
            return (Message) reader.readObject();
        } catch (IOException e) {
            System.err.println("[AuctionClient]> Connection to server lost: " + e.getMessage());
            return null;
        } catch (ClassNotFoundException e) {
            System.err.println("[AuctionClient]> Received unknown object format: " + e.getMessage());
            return null;
        }
    }

    /**
     * Atomically sends a request and waits for the specific reply.
     * Synchronized ensures that if the Poller is using the socket,
     * the User UI will wait until the Poller's full transaction is
     * finished.
     */
    public synchronized Message sendAndReceive(Message request) {
        sendMessage(request);
        return receiveMessage();
    }

    /**
     * Sets the session token. To be called by the CLI/UI after a successful LOGIN
     * request.
     *
     * @param tokenID The token UUID provided by the server.
     */
    public void setTokenID(String tokenID) {
        this.tokenID = tokenID;
    }

    /**
     * Starts a background polling thread that automatically requests the
     * currently active auction from the central server every 60 seconds.
     * <p>
     * When an active auction is received, this thread will print the item's
     * description and object ID to the console. It then triggers the
     * {@link #evaluateInterest(String)} method to simulate the user's
     * interest in the item.
     * <p>
     * This loop runs asynchronously on its own thread so it does not block
     * the main user interface. It will run indefinitely until
     * {@link #stopPolling()} is explicitly called (e.g. during logout).
     */
    public void startPolling() {
        isPolling = true;
        pollingThread = new Thread(() -> {
            System.out.println("[Poller]> Started background polling...");

            while (isPolling) {
                try {
                    Message req = new Message(Message.MessageType.GET_CURRENT_AUCTION);
                    Message response = sendAndReceive(req);

                    if (response != null && response.getType() == Message.MessageType.SUCCESS) {
                        String objId = response.getString("object_id");
                        String desc = response.getString("description");
                        System.out.println("\n[Poller]> Currently Auctioning: " + desc + " (ID: " + objId + ")");

                        evaluateInterest(objId);
                    }

                    Thread.sleep(60000);

                } catch (InterruptedException e) {
                    System.out.println("[Poller]> Polling interrupted/stopped.");
                }
            }
        });
        pollingThread.start();
    }

    public void stopPolling() {
        isPolling = false;
        if (pollingThread != null) {
            pollingThread.interrupt();
        }
    }

    /**
     * Safely closes the socket and streams when shutting down the client
     * application.
     */
    public void disconnect() {
        try {
            if (reader != null)
                reader.close();
            if (writer != null)
                writer.close();
            if (requestSocket != null && !requestSocket.isClosed())
                requestSocket.close();
            System.out.println("[AuctionClient]> Disconnected from server.");
        } catch (IOException e) {
            System.err.println("[AuctionClient]> Error during disconnect: " + e.getMessage());
        }
    }
}
