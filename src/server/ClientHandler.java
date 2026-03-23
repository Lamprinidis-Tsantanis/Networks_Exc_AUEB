package server;

import models.Message;
import models.Message.MessageType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Handles one peer connection on its own thread.
 * Reads Message objects from the socket, routes them to the correct manager,
 * and writes the response back.
 */
public class ClientHandler implements Runnable {

    private static final String TAG = "[ClientHandler]";

    private final Socket socket;
    private final AccountManager accountManager;
    private final DataStore dataStore;

    public ClientHandler(Socket socket, AccountManager accountManager, DataStore dataStore) {
        this.socket = socket;
        this.accountManager = accountManager;
        this.dataStore = dataStore;
    }

    // ----------------------------------------------------------------
    //  Main loop
    // ----------------------------------------------------------------

    @Override
    public void run() {
        String clientAddress = socket.getInetAddress().getHostAddress();
        int clientPort = socket.getPort();
        System.out.println(TAG + "> Handling client: " + clientAddress + ":" + clientPort);

        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream())) {

            while (true) {
                Message request = readMessage(in, clientAddress);
                if (request == null) break;

                System.out.println(TAG+"> Received "+ request.getType()+" from "+clientAddress+":"+clientPort);
                Message response = route(request, clientAddress, clientPort);
                send(out, response);
            }

        } catch (IOException e) {
            System.err.println(TAG + "> I/O error with " + clientAddress + ": " + e.getMessage());
        } finally {
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
    //  Router
    // ----------------------------------------------------------------

    /**
     * Dispatches the request to the correct handler based on its MessageType.
     */
    private Message route(Message request, String clientAddress, int clientPort) {
        return switch (request.getType()) {

            // -- Account management --
            case REGISTER  -> handleRegister(request);
            case LOGIN     -> handleLogin(request, clientAddress, clientPort);
            case LOGOUT    -> handleLogout(request);
            case CHECK_ACTIVE -> handleCheckActive(request);

            // -- Auction management --
            case REQUEST_AUCTION     -> handleRequestAuction(request);
            case GET_CURRENT_AUCTION -> handleGetCurrentAuction(request);
            case GET_AUCTION_DETAILS -> handleGetAuctionDetails(request);
            case PLACE_BID           -> handlePlaceBid(request);

            // Clients should NOT send SUCCESS/ERROR
            default -> error("Unsupported message type: " + request.getType());
        };
    }

    // ----------------------------------------------------------------
    //  Account handlers
    // ----------------------------------------------------------------

    private Message handleRegister(Message req) {
        String username = req.getString("username");
        String password = req.getString("password");

        if (username == null || password == null) {
            return error("Missing username or password.");
        }

        boolean ok = accountManager.register(username, password);
        if(ok){
            return success("Register successful.");
        }else{
            return error("Register failed. Account already exists.");
        }
    }

    private Message handleLogin(Message req, String clientAddress, int clientPort) {
        String username = req.getString("username");
        String password = req.getString("password");

        if (username == null || password == null) {
            return error("Missing username or password.");
        }

        String token = accountManager.login(username, password, clientAddress, clientPort);
        if (token != null) {
            Message resp = success("Login successful.");
            resp.put("token", token);
            return resp;
        }
        return error("Invalid credentials or user is already logged in.");
    }

    private Message handleLogout(Message req) {
        String token = req.getString("token");
        if (token == null) {return error("Missing token.");}
        if (!dataStore.isSessionActive(token)) {return error("Session not found or already expired.");}
        accountManager.logout(token);
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
    //  Auction handlers — delegates to AuctionManager (to be implemented)
    // ----------------------------------------------------------------

    private Message handleRequestAuction(Message req) {
        // TODO: validate session token, then delegate to AuctionManager.requestAuction(req)
        return error("REQUEST_AUCTION not yet implemented.");
    }

    private Message handleGetCurrentAuction(Message req) {
        // TODO: delegate to AuctionManager.getCurrentAuction()
        return error("GET_CURRENT_AUCTION not yet implemented.");
    }

    private Message handleGetAuctionDetails(Message req) {
        // TODO: delegate to AuctionManager.getAuctionDetails(req)
        return error("GET_AUCTION_DETAILS not yet implemented.");
    }

    private Message handlePlaceBid(Message req) {
        // TODO: validate session token, then delegate to AuctionManager.placeBid(req)
        return error("PLACE_BID not yet implemented.");
    }

    // ----------------------------------------------------------------
    //  Helpers
    // ----------------------------------------------------------------

    /**
     * Serializes a Message to the output stream.
     * Calls reset() after every write so that updated objects are not served
     * from ObjectOutputStream's internal reference cache on subsequent sends.
     */
    private void send(ObjectOutputStream out, Message message) {
        try {
            out.writeObject(message);
            out.flush();
            out.reset();
        } catch (IOException e) {
            System.err.println(TAG + "> Failed to send response: " + e.getMessage());
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
