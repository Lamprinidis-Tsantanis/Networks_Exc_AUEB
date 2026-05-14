package peer;

import models.Message;
import utils.Constants;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

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
    private String username = null;
    private boolean isPolling = false;
    private volatile boolean isDisconnecting = false;
    private Thread pollingThread;
    private String directoryPath;

    private final java.util.concurrent.LinkedBlockingQueue<Message> responseBuffer = new java.util.concurrent.LinkedBlockingQueue<>();
    private final java.util.concurrent.LinkedBlockingQueue<Message> notificationBuffer = new java.util.concurrent.LinkedBlockingQueue<>();
    private Thread responseReaderThread;
    private Thread notificationDispatchThread;

    private final Object writeLock = new Object();

    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Connects to the AuctionServer using the IP and Port defined in
     * Constants.java.
     * Initializes the output and input streams for object serialization.
     * Starts a background reader thread and a dedicated notification dispatcher
     * thread.
     *
     * @return {@code true} if connection was successful, {@code false} otherwise.
     */
    public boolean connect(String directoryPath) {
        this.directoryPath = directoryPath;
        try {
            requestSocket = new Socket(Constants.SERVER_IP, Constants.SERVER_PORT);
            writer = new ObjectOutputStream(requestSocket.getOutputStream());
            writer.flush();
            reader = new ObjectInputStream(requestSocket.getInputStream());

            responseReaderThread = new Thread(() -> {
                while (true) {
                    try {
                        Message msg = (Message) reader.readObject();
                        if (msg == null)
                            continue;

                        if (msg.getType() == Message.MessageType.AUCTION_RESULT
                                || msg.getType() == Message.MessageType.CHECK_ACTIVE) {
                            notificationBuffer.offer(msg);
                        } else {
                            responseBuffer.offer(msg);
                        }

                    } catch (IOException | ClassNotFoundException e) {
                        if (!isDisconnecting) {
                            System.err.println("[AuctionClient]> Reader thread error: " + e.getMessage());
                        }
                        break;
                    }
                }
            });
            responseReaderThread.setDaemon(true);
            responseReaderThread.start();

            notificationDispatchThread = new Thread(() -> {
                while (true) {
                    try {
                        Message notification = notificationBuffer.take();
                        handleNotification(notification);
                    } catch (InterruptedException e) {
                        System.out.println("[AuctionClient]> Notification dispatcher stopped.");
                        break;
                    }
                }
            });
            notificationDispatchThread.setDaemon(true);
            notificationDispatchThread.start();

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
     * Handles asynchronous notifications pushed by the server (AUCTION_RESULT,
     * CHECK_ACTIVE).
     * Runs exclusively on the notificationDispatchThread so it never races with
     * sendAndReceive().
     */
    private void handleNotification(Message msg) {
        if (msg.getType() == Message.MessageType.CHECK_ACTIVE) {
            return;
        }

        String status = msg.getString("status");
        if (status == null)
            return;

        System.out.println("[AuctionClient]> NOTIFICATION [" + status + "]: " + msg.getString("message"));

        switch (status) {
            case "WON":
                String sellerIp = msg.getString("p2pIpAddress");
                int sellerPort = (Integer) msg.get("p2pPort");
                String objId = msg.getString("object_id");
                double price = Double.parseDouble(msg.get("final_price").toString());
                System.out.println("[AuctionClient]> You WON " + objId + "! Launching Transaction...");
                new Thread(new TransactionHandler(sellerIp, sellerPort, objId, price, directoryPath, this)).start();
                break;

            case "NEW_HIGHEST_BID":
                System.out.println("[AuctionClient]> New highest bid on " + msg.getString("object_id")
                        + ": " + msg.get("current_bid"));
                break;

            case "AUCTION_CANCELLED":
                System.out.println("[AuctionClient]> The current auction was cancelled (seller disconnected).");
                break;

            case "SOLD":
                System.out.println("[AuctionClient]> Your item " + msg.getString("object_id")
                        + " was sold for " + msg.get("final_price")
                        + " to " + msg.getString("buyer_username") + ".");
                break;

            case "NO_BIDS":
                System.out.println("[AuctionClient]> Your item " + msg.getString("object_id")
                        + " received no bids.");
                break;

            default:
                System.out.println("[AuctionClient]> Unhandled notification status: " + status);
                break;
        }
    }

    /**
     * Sends a Message object to the server.
     * Automatically includes the token_id in the payload if the user is logged in.
     *
     * @param msg The Message object to be sent.
     */
    public void sendMessage(Message msg) {
        synchronized (writeLock) {
            try {
                if (tokenID != null) {
                    msg.put("token", tokenID);
                }
                writer.writeObject(msg);
                writer.flush();
                writer.reset();
            } catch (IOException e) {
                System.err.println("[AuctionClient]> Error sending message: " + e.getMessage());
            }
        }
    }

    /**
     * Blocks and waits to receive a Message object from the server.
     *
     * @return The received Message object, or {@code null} if the connection
     *         dropped.
     */
    public Message receiveMessage() {
        try {
            return responseBuffer.poll(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("[AuctionClient]> Interrupted: " + e.getMessage());
            return null;
        }
    }

    /**
     * Atomically sends a request and waits for the reply from the responseBuffer.
     * Notifications (AUCTION_RESULT, CHECK_ACTIVE) are handled by the dedicated
     * dispatcher thread and never appear in this buffer.
     */
    public Message sendAndReceive(Message request) {
        synchronized (writeLock) {
            try {
                if (tokenID != null) {
                    request.put("token", tokenID);
                }
                writer.writeObject(request);
                writer.flush();
                writer.reset();
            } catch (IOException e) {
                System.err.println("[AuctionClient]> Error sending message: " + e.getMessage());
                return null;
            }
        }

        try {
            Message response = responseBuffer.poll(30, java.util.concurrent.TimeUnit.SECONDS);
            if (response == null) {
                System.err.println("[AuctionClient]> Timeout waiting for response");
            }
            return response;
        } catch (InterruptedException e) {
            System.err.println("[AuctionClient]> Interrupted waiting for response: " + e.getMessage());
            return null;
        }
    }

    /**
     * Sets the session token. Called after a successful LOGIN.
     *
     * @param tokenID The token UUID provided by the server.
     */
    public void setTokenID(String tokenID) {
        this.tokenID = tokenID;
    }

    /**
     * Starts a background polling thread that requests the currently active auction
     * from the server every 60 seconds and evaluates bidding interest.
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
                        Object auctionsObj = response.get("auctions");

                        if (auctionsObj instanceof List) {
                            List<?> auctions = (List<?>) auctionsObj;

                            if (auctions.isEmpty()) {
                                System.out.println("[Poller]> No active auctions.");
                            } else {
                                System.out.println("[Poller]> Current auctions (" + auctions.size() + " active):");
                                for (Object auctionObj : auctions) {
                                    if (auctionObj instanceof Map) {
                                        @SuppressWarnings("unchecked")
                                        Map<String, String> auctionMap = (Map<String, String>) auctionObj;
                                        String objId = auctionMap.get("object_id");
                                        String desc = auctionMap.get("description");
                                        System.out.println("  - " + desc + " (ID: " + objId + ")");
                                        evaluateInterest(objId);
                                    }
                                }
                            }
                        }
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
     * Simulates bidder interest using a 60% probability coin flip.
     * If interested, fetches auction details and places a bid using the formula:
     * NewBid = HighestBid * (1 + RAND/10).
     *
     * @param objId The ID of the item currently up for auction.
     */
    public void evaluateInterest(String objId) {
        double randInterest = Math.random();

        if (this.username != null && objId.contains(this.username)) {
            System.out.println("[Poller]> Skipping item " + objId + " because I am the seller.");
            return;
        }

        if (randInterest < 0.6) {
            System.out
                    .println("[Poller]> 60% Check Passed! I am interested in item " + objId + ". Fetching details...");

            Message reqDetails = new Message(Message.MessageType.GET_AUCTION_DETAILS);
            reqDetails.put("object_id", objId);
            Message response = sendAndReceive(reqDetails);

            if (response != null && response.getType() == Message.MessageType.SUCCESS) {
                String sellerToken = response.getString("seller_token");
                Object highestBid = response.get("highest_bid");
                Object timeRemainingObj = response.get("remaining_time");
                Object totalDurationObj = response.get("total_duration");

                long timeRemaining = ((Number) timeRemainingObj).longValue();
                long totalDuration = ((Number) totalDurationObj).longValue();

                System.out.println("   --- AUCTION DETAILS ---");
                System.out.println("   -> Seller Token: " + sellerToken);
                System.out.println("   -> Highest Bid:  " + highestBid);
                System.out.println("   -> Time Left:    " + timeRemaining + " seconds");
                System.out.println("   -----------------------");

                try {
                    double currentHighest = ((Number) highestBid).doubleValue();
                    double newBid;

                    /*
                     * Check the remaining_time to fing out if it is 10%
                     * and increase the value to max 20%
                     */
                    if (timeRemaining <= (0.10 * totalDuration)) {
                        newBid = currentHighest * (1 + (Math.random() * 0.20));
                        System.out.println("[Poller]> Final 10% of auction reached! Aggressive bidding activated.");
                    } else {
                        newBid = currentHighest * (1 + (Math.random() * 0.10));
                    }

                    newBid = Math.round(newBid * 100.0) / 100.0;

                    Message placeBid = new Message(Message.MessageType.PLACE_BID);
                    placeBid.put("object_id", objId);
                    placeBid.put("bid_amount", newBid);

                    System.out.println("[Poller]> Attempting to place bid of " + newBid + "...");

                    Message bidResponse = sendAndReceive(placeBid);

                    if (bidResponse != null && bidResponse.getType() == Message.MessageType.SUCCESS) {
                        System.out.println("[Poller]> Bid placed successfully! New highest bid is: " + newBid);
                    } else {
                        String errorMsg = (bidResponse != null) ? bidResponse.getString("message") : "Connection lost";
                        System.out.println("[Poller]> Failed to place bid for " + objId + ". Reason: " + errorMsg);
                    }
                } catch (Exception e) {
                    System.err.println("[Poller]> Error calculating or placing bid: " + e.getMessage());
                }
            } else {
                System.out.println("[Poller]> Failed to get details. The auction might have just ended.");
            }
        } else {
            System.out.println("[Poller]> Not interested in item " + objId + " this time.");
        }
    }

    /**
     * Safely closes the socket and streams when shutting down the client
     * application.
     */
    public void disconnect() {
        isDisconnecting = true;
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
