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
    private final ConcurrentHashMap<String, UserRecord> registeredUsers = new ConcurrentHashMap<>();
    /** TokenId is Key */
    private final ConcurrentHashMap<String, SessionRecord> activeSessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ClientHandler> activeClientHandlers = new ConcurrentHashMap<>();

    private final LinkedBlockingDeque<AuctionEntry> auctionQueue = new LinkedBlockingDeque<>();

    // ---------------------------------------------------------
    // SINGLETON
    // ---------------------------------------------------------
    private static DataStore singletonInstance = null;

    private DataStore() {
    }

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
        public double reputation_score;

        public UserRecord(String password) {
            this.password = password;
            this.numAuctionsSeller = 0;
            this.numAuctionsBidder = 0;
            this.reputation_score = 1.0;
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
         * @param username     username of user
         * @param ipAddress    ipAddress of user
         * @param port         port where user transmits
         * @param p2pIpAddress ipAddress where user waits for buyer
         * @param p2pPort      port where user waits for buyer
         */
        public SessionRecord(String username, String ipAddress, int port, String p2pIpAddress, int p2pPort) {
            this.username = username;
            this.ipAddress = ipAddress;
            this.port = port;
            this.p2pIpAddress = p2pIpAddress;
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
    public boolean userExists(String username) {
        return registeredUsers.containsKey(username);
    }

    /** Adds user to registeredUsers after checking for duplicate username */
    public boolean registerUser(String username, String password) {
        if (registeredUsers.putIfAbsent(username, new UserRecord(password)) == null) {
            System.out.println("[DataStore]> User " + username + " has been registered successfully.");
            return true;
        }
        return false;
    }

    /** Returns True if username and password match records */
    public boolean validateUser(String username, String password) {
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

    public double getReputation(String username) {
        UserRecord record = registeredUsers.get(username);
        return record.reputation_score;
    }

    public void updateReputation(String username, boolean isSuccessfull) {
        UserRecord record = registeredUsers.get(username);
        double oldScore = getReputation(username);
        if (isSuccessfull) {
            record.reputation_score = (1 - 0.25) * oldScore + 0.25;
            System.out.println("\n════════════════════════════════════════════════════════════");
            System.out.println("[DataStore]> REPUTATION INCREASED (TRANSACTION SUCCESSFUL)");

        } else {
            record.reputation_score = (1 - 0.25) * getReputation(username);
            System.out.println("\n════════════════════════════════════════════════════════════");
            System.out.println("[DataStore]> REPUTATION DECREASED (TRANSACTION CANCELLED)");

        }
        System.out.println("    User: " + username);
        System.out.println("    BEFORE: " + String.format("%.4f", oldScore));
        System.out.println("    AFTER:  " + String.format("%.4f", record.reputation_score));
        System.out.println("════════════════════════════════════════════════════════════\n");
    }

    // ----------------------------------------------------------
    // SESSION METHODS
    // ----------------------------------------------------------
    /** Creates a new session, returns false if the session already exists */
    public boolean addSession(String tokenId, String username, String ipAddress, int port, String p2pIpAddress,
            int p2pPort) {
        if (activeSessions.putIfAbsent(tokenId,
                new SessionRecord(username, ipAddress, port, p2pIpAddress, p2pPort)) == null) {
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
    public boolean isUserLoggedIn(String username) {
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

    /**
     * Safely dequeues the next item for auction by comparing the reputation scores
     * of the owners of the first two items in the queue.
     * <p>
     * This method blocks until at least one item is available. If a second item
     * exists in the queue, it compares the reputation scores of the two sellers.
     * The item belonging to the seller with the higher (or equal) reputation is
     * selected, while the other item is placed back at the front of the queue.
     * <p>
     * If the queue contains only one item, it is returned immediately without
     * comparison.
     *
     * @return The {@link AuctionEntry} selected to be auctioned next based on
     *         reputation.
     * @throws InterruptedException if the thread is interrupted while waiting for
     *                              an item to become available.
     */
    public AuctionEntry dequeueItemWithReputation() throws InterruptedException {
        AuctionEntry firstItem = auctionQueue.take();
        AuctionEntry secondItem = auctionQueue.poll();

        if (secondItem == null) {
            return firstItem;
        }

        String firstTokenId = firstItem.sellerTokenId;
        String secondTokenId = secondItem.sellerTokenId;

        String firstUsername = getUsernameByToken(firstTokenId);
        String secondUsername = getUsernameByToken(secondTokenId);

        double firstScore = getReputation(firstUsername);
        double secondScore = getReputation(secondUsername);

        if (firstScore < secondScore) {
            auctionQueue.addFirst(firstItem);
            System.out.println("Promoting item " +
                    secondItem.auctionItem.getObjectId()
                    + " over item " + firstItem.auctionItem.getObjectId()
                    + " due to higher reputation (" + secondScore + " > " + firstScore + ")");
            return secondItem;
        } else {
            auctionQueue.addFirst(secondItem);
            System.out.println("Promoting item " + firstItem.auctionItem.getObjectId()
                    + " over item " + secondItem.auctionItem.getObjectId()
                    + " due to higher reputation (" + firstScore + " > " + secondScore + ")");
            return firstItem;
        }
    }
}
