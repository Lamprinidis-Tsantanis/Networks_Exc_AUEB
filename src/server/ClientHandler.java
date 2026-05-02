package server;

import models.Message;
import models.Message.MessageType;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles one peer connection on its own thread.
 * Reads Message objects from the socket, routes them to the correct manager,
 * and writes the response back.
 */
public class ClientHandler implements Runnable {

    private static final String TAG = "[ClientHandler]";

    private final Socket socket;
    private final AccountManager accountManager;
    private final AuctionManager auctionManager;
    private final DataStore dataStore;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private String currentToken;

    public ClientHandler(Socket socket, AccountManager accountManager, AuctionManager auctionManager,
            DataStore dataStore) {
        this.socket = socket;
        this.accountManager = accountManager;
        this.auctionManager = auctionManager;
        this.dataStore = dataStore;
    }

    // ----------------------------------------------------------------
    // Main loop
    // ----------------------------------------------------------------

    @Override
    public void run() {
        String clientAddress = socket.getInetAddress().getHostAddress();
        int clientPort = socket.getPort();
        System.out.println(TAG + "> Handling client: " + clientAddress + ":" + clientPort);

        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            while (true) {
                Message request = readMessage(in, clientAddress);
                if (request == null)
                    break;

                System.out
                        .println(TAG + "> Received " + request.getType() + " from " + clientAddress + ":" + clientPort);
                Message response = route(request, clientAddress, clientPort);
                send(out, response);
            }

        } catch (IOException e) {
            System.err.println(TAG + "> I/O error with " + clientAddress + ": " + e.getMessage());
        } finally {
            if (currentToken != null) {
                System.out
                        .println(TAG + "> Abrupt disconnect detected. Cleaning up session for token: " + currentToken);
                accountManager.logout(currentToken);
                dataStore.unregisterClientHandler(currentToken);
            }
            closeQuietly();
        }
    }

    /**
     * Attempts to deserialize one Message from the stream.
     *
     * @return the Message, or null if the stream is closed / class is unknown.
     */
    private Message readMessage(ObjectInputStream in, String clientAddress) {
        try {
            return (Message) in.readObject();
        } catch (ClassNotFoundException e) {
            System.err.println(TAG + "> Unknown class from " + clientAddress + ": " + e.getMessage());
            return null;
        } catch (IOException e) {
            // Normal EOF when the peer closes the connection — not an error.
            System.out.println(TAG + "> Client disconnected: " + clientAddress);
            return null;
        }
    }

    // ----------------------------------------------------------------
    // Router
    // ----------------------------------------------------------------

    /**
     * Dispatches the request to the correct handler based on its MessageType.
     */
    private Message route(Message request, String clientAddress, int clientPort) {
        return switch (request.getType()) {

            // -- Account management --
            case REGISTER -> handleRegister(request);
            case LOGIN -> handleLogin(request, clientAddress, clientPort);
            case LOGOUT -> handleLogout(request);
            case CHECK_ACTIVE -> handleCheckActive(request);

            // -- Auction management --
            case REQUEST_AUCTION -> handleRequestAuction(request);
            case GET_CURRENT_AUCTION -> handleGetCurrentAuctions(request);
            case GET_AUCTION_DETAILS -> handleGetAuctionDetails(request);
            case PLACE_BID -> handlePlaceBid(request);
            case CONFIRM_OWNERSHIP -> handleOwnership(request);

            // Clients should NOT send SUCCESS/ERROR
            default -> error("Unsupported message type: " + request.getType());
        };
    }

    // ----------------------------------------------------------------
    // Account handlers
    // ----------------------------------------------------------------

    private Message handleRegister(Message req) {
        String username = req.getString("username");
        String password = req.getString("password");

        if (username == null || password == null) {
            return error("Missing username or password.");
        }

        username = username.toLowerCase();

        boolean ok = accountManager.register(username, password);
        if (ok) {
            return success("Register successful.");
        } else {
            return error("Register failed. Account already exists.");
        }
    }

    private Message handleLogin(Message req, String clientAddress, int clientPort) {
        String username = req.getString("username");
        String password = req.getString("password");
        String p2pIpAddress = req.getString("p2pIpAddress");

        Integer p2pPort = null;
        Object portObj = req.get("p2pPort");
        if (portObj instanceof Integer) {
            p2pPort = (Integer) portObj;
        } else if (portObj instanceof String) {
            try {
                p2pPort = Integer.parseInt((String) portObj);
            } catch (NumberFormatException ignored) {
            }
        }

        if (username == null || password == null || p2pIpAddress == null || p2pPort == null) {
            return error("Missing username, password, or peer listening info."); // <--- UPDATED
        }

        username = username.toLowerCase();

        String token = accountManager.login(username, password, clientAddress, clientPort, p2pIpAddress, p2pPort);
        if (token != null) {
            this.currentToken = token;
            dataStore.registerClientHandler(token, this);
            Message resp = success("Login successful.");
            resp.put("token", token);
            return resp;
        }
        return error("Invalid credentials or user is already logged in.");
    }

    private Message handleLogout(Message req) {
        String token = req.getString("token");
        if (token == null) {
            return error("Missing token.");
        }
        if (!dataStore.isSessionActive(token)) {
            return error("Session not found or already expired.");
        }
        accountManager.logout(token);
        dataStore.unregisterClientHandler(token);

        if (token.equals(this.currentToken)) {
            this.currentToken = null;
        }

        return success("Logout successful.");
    }

    private Message handleCheckActive(Message req) {
        String token = req.getString("token");
        boolean isActive = (token != null) && dataStore.isSessionActive(token);
        Message resp = success("Session status retrieved.");
        resp.put("active", isActive);
        return resp;
    }

    // ----------------------------------------------------------------
    // Auction handlers
    // ----------------------------------------------------------------

    private Message handleRequestAuction(Message req) {
        String token = req.getString("token");
        // Warning: This assumes the client passed a List<Item> inside the payload
        @SuppressWarnings("unchecked")
        java.util.List<models.Item> items = (java.util.List<models.Item>) req.get("items");

        boolean ok = auctionManager.getAuctionRequest(token, items);
        if (ok)
            return success("Items added to auction queue.");
        return error("Failed to add items to queue. Invalid token or empty list.");
    }

    private Message handleGetCurrentAuctions(Message req) {
        List<String[]> auctions = auctionManager.getAllCurrentAuctions();
        if (auctions.isEmpty()) {
            return error("No active auctions.");
        }
        Message resp = success("Current auctions retrieved.");
        // Create a list of auction objects
        List<Map<String, String>> auctionList = new ArrayList<>();
        for (String[] auction : auctions) {
            Map<String, String> auctionMap = new HashMap<>();
            auctionMap.put("object_id", auction[0]);
            auctionMap.put("description", auction[1]);
            auctionList.add(auctionMap);
        }
        resp.put("auctions", auctionList);
        return resp;
    }

    private Message handleGetAuctionDetails(Message req) {
        String objectId = req.getString("object_id");

        List<Object[]> auctionList = auctionManager.getAllAuctionDetails();
        if (auctionList == null)
            return error("No active auction.");
        for (Object[] details : auctionList) {
            if (details[4].equals(objectId)) {
                Message resp = success("Auction details retrieved.");
                resp.put("seller_token", details[0]);
                resp.put("highest_bid", details[1]);
                resp.put("remaining_time", details[2]);
                resp.put("total_duration", details[3]);
                resp.put("object_id", details[4]);
                return resp;
            }
        }
        return error("No active auction found for item: " + objectId);
    }

    private Message handlePlaceBid(Message req) {
        String token = req.getString("token");
        String objectId = req.getString("object_id");

        // Safely extract double from the payload
        Double bidAmount = null;
        Object val = req.get("bid_amount");
        if (val instanceof Double)
            bidAmount = (Double) val;
        else if (val instanceof Integer)
            bidAmount = ((Integer) val).doubleValue();

        if (token == null || objectId == null || bidAmount == null)
            return error("Missing parameters.");

        return auctionManager.placeBid(token, objectId, bidAmount);
    }

    private Message handleOwnership(Message request) {
        String token = request.getString("token");
        String objectId = request.getString("object_id");

        if (token == null || objectId == null) {
            return error("Missing token or object_id in ownership confirmation.");
        }

        if (!dataStore.isSessionActive(token)) {
            return error("Session not found or already expired.");
        }

        System.out.println(TAG + "> Ownership of item " + objectId
                + " confirmed for token: " + token);

        return success("Ownership confirmed. Item registered to buyer.");
    }

    // ----------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------

    /**
     * Serializes a Message to the output stream.
     * Calls reset() after every write so that updated objects are not served
     * from ObjectOutputStream's internal reference cache on subsequent sends.
     */
    private void send(ObjectOutputStream out, Message message) {
        synchronized (this) {
            try {
                out.writeObject(message);
                out.flush();
                out.reset();
            } catch (IOException e) {
                System.err.println(TAG + "> Failed to send response: " + e.getMessage());
            }
        }
    }

    /**
     * Sends a Message to the client.
     * Called by AuctionManagerThread to broadcast auction updates.
     *
     * @param message The message to send to the client
     * @throws IOException if the send fails
     */
    public synchronized void sendMessage(Message message) throws IOException {
        if (out != null) {
            out.writeObject(message);
            out.flush();
            out.reset();
        }
    }

    private static Message success(String text) {
        Message msg = new Message(MessageType.SUCCESS);
        msg.put("message", text);
        return msg;
    }

    private static Message error(String text) {
        Message msg = new Message(MessageType.ERROR);
        msg.put("message", text);
        return msg;
    }

    private void closeQuietly() {
        try {
            socket.close();
        } catch (IOException e) {
            System.err.println(TAG + "> Failed to close socket: " + e.getMessage());
        }
    }
}
