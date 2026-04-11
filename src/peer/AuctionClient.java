package peer;

import models.Message;
import utils.Constants;

import java.awt.TrayIcon.MessageType;
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
    private String directoryPath;

    private final java.util.concurrent.LinkedBlockingQueue<Message> responseBuffer =
            new java.util.concurrent.LinkedBlockingQueue<>();
    private Thread responseReaderThread;

    private final Object socketLock = new Object();

    /**
     * Connects to the AuctionServer using the IP and Port defined in
     * Constants.java.
     * Initializes the output and input streams for object serialization.
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

            // START response reader thread
            responseReaderThread = new Thread(() -> {
                while (true) {
                    try {
                        Message msg = (Message) reader.readObject();
                        if (msg != null) {
                            responseBuffer.offer(msg);
                        }

                    } catch (IOException | ClassNotFoundException e) {
                        System.err.println("[AuctionClient]> Reader thread error: " + e.getMessage());
                        break;
                    }
                }
            });
            responseReaderThread.setDaemon(true);
            responseReaderThread.start();

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
    public synchronized void sendMessage(Message msg) {
        synchronized (socketLock) {
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
     *         dropped or failed.
     */
    public Message receiveMessage() {
        try {
            // Use timeout to prevent hanging
            return responseBuffer.poll(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("[AuctionClient]> Interrupted: " + e.getMessage());
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
        synchronized (socketLock) {
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
            long deadline = System.currentTimeMillis() + 30000; // 30-second timeout

            // Loop through the buffer and ignore async notifications
            while (System.currentTimeMillis() < deadline) {
                Message response = responseBuffer.poll(1, java.util.concurrent.TimeUnit.SECONDS);

                if (response != null) {
                    // 1. Check for Asynchronous Notifications (Like Winning)
                    if (response.getType() == Message.MessageType.AUCTION_RESULT) {
                        String status = response.getString("status");
                        System.out.println("[AuctionClient]> NOTIFICATION: " + response.getString("message"));

                        if ("WON".equals(status)) {
                            String sellerIp = response.getString("p2pIpAddress");
                            int sellerPort = Integer.parseInt(response.get("p2pPort").toString());
                            String objId = response.getString("object_id");
                            double price = Double.parseDouble(response.get("final_price").toString());

                            System.out.println("[AuctionClient]> You WON " + objId + "! Launching Transaction...");

                            // Start the P2P transfer
                            new Thread(new TransactionHandler(
                                    sellerIp, sellerPort, objId, price, directoryPath, this
                            )).start();
                        }

                        // This was an async notification, so we 'continue' to keep waiting
                        // for the actual response to the request we sent.
                        continue;
                    }

                    // 2. Check for other server pings (e.g., heartbeat)
                    if (response.getType() == Message.MessageType.CHECK_ACTIVE) {
                        continue;
                    }

                    // 3. If it's not a notification, it's the SUCCESS/ERROR reply we wanted
                    return response;
                }
            }
            System.err.println("[AuctionClient]> Timeout waiting for response");
            return null;
        } catch (InterruptedException e) {
            System.err.println("[AuctionClient]> Interrupted waiting for response: " + e.getMessage());
            return null;
        }
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
                        System.out.println("[Poller]> Currently Auctioning: " + desc + " (ID: " + objId + ")");

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
     * Simulates bidder interest in a currently active auction using probability.
     * <p>
     * This method evaluates a 60% random chance to determine if the peer is
     * interested in the specified item. If the peer is interested, it constructs
     * and sends a {@code GET_AUCTION_DETAILS} request to the central server to
     * retrieve the current state of the auction (seller token, highest bid, and
     * remaining time) and displays it to the user.
     * <p>
     * This method acts as the entry point for automated bidding and sets up the
     * state required to place a formal bid.
     *
     * @param objId The ID of the item currently up for auction.
     */
    public void evaluateInterest(String objId) {
        double randInterest = Math.random();

        if (randInterest < 0.6) {
            System.out
                    .println("[Poller]> 60% Check Passed! I am interested in item " + objId + ". Fetching details...");

            Message reqDetails = new Message(Message.MessageType.GET_AUCTION_DETAILS);
            reqDetails.put("object_id", objId);
            Message response = sendAndReceive(reqDetails);

            if (response != null && response.getType() == Message.MessageType.SUCCESS) {
                // SUCCESS part
                String sellerToken = response.getString("seller_token");
                Object highestBid = response.get("highest_bid");
                Object timeRemaining = response.get("remaining_time");

                System.out.println("   --- AUCTION DETAILS ---");
                System.out.println("   -> Seller Token: " + sellerToken);
                System.out.println("   -> Highest Bid:  " + highestBid);
                System.out.println("   -> Time Left:    " + timeRemaining + " seconds");
                System.out.println("   -----------------------");

                // PLACE BID part
                try {
                    double currentHighest = ((Number) highestBid).doubleValue();
                    double newBid = currentHighest * (1 + (Math.random() / 10));
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
