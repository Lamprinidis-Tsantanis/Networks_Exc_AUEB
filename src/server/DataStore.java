package server;

import models.Item;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Class that manages registeredUsers (usernames and passwords), connections
 * (sections, tokenId) and an Item list (auctionList)
 */
public class DataStore {
    /** username is Key */
    private static final ConcurrentHashMap<String, UserRecord> registeredUsers = new ConcurrentHashMap<>();

    /** TokenId is Key */
    private static final ConcurrentHashMap<String, SessionRecord> activeSessions = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ClientHandler> activeClientHandlers = new ConcurrentHashMap<>();

    private final LinkedBlockingDeque<AuctionEntry> auctionQueue = new LinkedBlockingDeque<>();

    // ---------------------------------------------------------
    // SINGLETON
    // ---------------------------------------------------------
    private static DataStore singletonInstance = null;

    private DataStore() {}

    public synchronized static DataStore getInstance() {
        if (singletonInstance == null) {
            singletonInstance = new DataStore();
        }
        return singletonInstance;
    }

    // ----------------------------------------------------------
    // DATA CLASSES
    // ----------------------------------------------------------
    public static class UserRecord {
        public String password;
        public int numAuctionsSeller;
        public int numAuctionsBidder;

        public UserRecord(String password) {
            this.password = password;
            this.numAuctionsSeller = 0;
            this.numAuctionsBidder = 0;
        }
    }

    public static class SessionRecord {
        public String username;
        public String ipAddress;
        public int port;
        public String p2pIpAddress;
        public int p2pPort;

        /**
         * creates sessionRecord <br>
         * the token id is not registered in here because it is the key of the hashmap
         * {@code activeSessions}
         * 
         * @param username      username of user
         * @param ipAddress     ipAddress of user
         * @param port          port where user transmits
         * @param p2pIpAddress  ipAddress where user waits for buyer
         * @param p2pPort       port where user waits for buyer
         */
        public SessionRecord(String username, String ipAddress, int port, String p2pIpAddress, int p2pPort) {
            this.username = username;
            this.ipAddress = ipAddress;
            this.port = port;
            this.p2pIpAddress=p2pIpAddress;
            this.p2pPort = p2pPort;
        }
    }

    public static class AuctionEntry {
        public Item auctionItem;
        public String sellerTokenId;
        public AuctionEntry(Item auctionItem, String sellerTokenId) {
            this.auctionItem = auctionItem;
            this.sellerTokenId = sellerTokenId;
        }
    }

    // ----------------------------------------------------------
    // USER METHODS
    // ----------------------------------------------------------

    /** Returns True if user exists */
    public static boolean userExists(String username) {
        return registeredUsers.containsKey(username);
    }

    /** Adds user to registeredUsers after checking for duplicate username */
    public boolean registerUser(String username, String password) {
        if (userExists(username)) {
            return false;
        }
        registeredUsers.put(username, new UserRecord(password));
        System.out.println("[DataStore]> User " + username + " has been registered successfully.");
        return true;
    }

    /** Returns True if username and password match records */
    public static boolean validateUser(String username, String password) {
        UserRecord record = registeredUsers.get(username);
        return record != null && record.password.equals(password);
    }

    /**
     * Increments users BidderCount by 1, Returns False if username doesn't exist
     */
    public void addBidderCount(String username) {
        UserRecord record = registeredUsers.get(username);
        if (record != null) {
            record.numAuctionsBidder++;
        }
    }

    /**
     * Increments users SellerCount by 1, Returns False if username doesn't exist
     */
    public void addSellerCount(String username) {
        UserRecord record = registeredUsers.get(username);
        if (record != null) {
            record.numAuctionsSeller++;
        }
    }

    // ----------------------------------------------------------
    // SESSION METHODS
    // ----------------------------------------------------------
    /** Creates a new session, returns false if the session already exists */
    public static boolean addSession(String tokenId, String username, String ipAddress, int port, String p2pIpAddress, int p2pPort) {
        if (!activeSessions.containsKey(tokenId)) {
            activeSessions.put(tokenId, new SessionRecord(username, ipAddress, port,p2pIpAddress, p2pPort));
            System.out.println("[DataStore]> Session " + username + " has been registered successfully.");
            return true;
        }
        return false;
    }

    /** Checks if Session is recorded in activeSessions */
    public boolean isSessionActive(String tokenId) {
        return activeSessions.containsKey(tokenId);
    }

    /** removes Session mapped with said tokenId from activeSessions */
    public void removeSession(String tokenId) {
        SessionRecord removed = activeSessions.remove(tokenId);
        if (removed != null) {
            System.out.println("[DataStore]> Session removed — token: " + tokenId + " | user: " + removed.username);
        }
    }

    /** Returns session based on tokenId */
    public SessionRecord getSession(String tokenId) {
        return activeSessions.get(tokenId);
    }


    /** Returns username based on tokenId */
    public String getUsernameByToken(String tokenId) {
        SessionRecord record = activeSessions.get(tokenId);
        return record != null ? record.username : null;
    }

    /** Returns true if user has an active session */
    public static boolean isUserLoggedIn(String username) {
        for (SessionRecord s : activeSessions.values()) {
            if (s.username.equals(username))
                return true;
        }
        return false;
    }

    public void registerClientHandler(String tokenId, ClientHandler handler) {
        activeClientHandlers.put(tokenId, handler);
    }

    public ClientHandler getClientHandler(String tokenId) {
        return activeClientHandlers.get(tokenId);
    }

    public void unregisterClientHandler(String tokenId) {
        ClientHandler removed = activeClientHandlers.remove(tokenId);
        if (removed != null) {
            System.out.println("[DataStore]> ClientHandler removed for token: " + tokenId);
        }
    }
    // ----------------------------------------------------------
    // AUCTION QUEUE METHODS
    // ----------------------------------------------------------

    /** Adds an item to the end off the auction queue */
    public void enqueueItem(Item item, String sellerTokenId) {
        if (auctionQueue.offer(new AuctionEntry(item, sellerTokenId))) {
            System.out.println(
                    "[DataStore] Item enqueued: " + item.getObjectId() + " | Queue size: " + auctionQueue.size());
            return;
        }
        System.out.println("[DataStore] Item NOT enqueued: " + item.getObjectId());
    }

    /** Removes and returns the first item of the queue */
    public AuctionEntry dequeueItem() throws InterruptedException {
        return auctionQueue.take();
    }

    /** returns the size of the queue */
    public int getQueueSize() {
        return auctionQueue.size();
    }

    /** Returns the map of all active sessions */
    public java.util.concurrent.ConcurrentHashMap<String, SessionRecord> getActiveSessions() {
        return activeSessions;
    }
}
