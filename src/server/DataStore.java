package server;

import models.Item;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/** Class that manages registeredUsers (usernames and passwords), connections (sections, tokenId) and an Item list (auctionList)*/
public class DataStore {
    private final ConcurrentHashMap<String, UserRecord> registeredUsers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SessionRecord> activeSessions = new ConcurrentHashMap<>();
    private final LinkedBlockingDeque<Item> auctionQueue = new LinkedBlockingDeque<>();

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
        public SessionRecord(String username, String ipAddress, int port) {
            this.username = username;
            this.ipAddress = ipAddress;
            this.port = port;
        }
    }

//----------------------------------------------------------
//              USER METHODS
//----------------------------------------------------------

    /** Returns True if user exists */
    public boolean userExists(String username) {
        return registeredUsers.containsKey(username);
    }
    /** Adds user to registeredUsers after checking for duplicate username*/
    public boolean registerUser(String username, String password) {
        if (userExists(username)) {return false;}
        registeredUsers.put(username, new UserRecord(password));
        System.out.println("[DataStore]> User " + username + " has been registered successfully.");
        return true;
    }
    /** Returns True if username and password match records*/
    public boolean validateUser(String username, String password) {
        UserRecord record = registeredUsers.get(username);
        return record != null && record.password.equals(password);
    }
    /** Increments users BidderCount by 1, Returns False if username doesn't exist*/
    public boolean addBidderCount(String username) {
        UserRecord record = registeredUsers.get(username);
        if (record != null){record.numAuctionsBidder++;return true;}
        return false;
    }
    /** Increments users SellerCount by 1, Returns False if username doesn't exist*/
    public boolean addSellerCount(String username) {
        UserRecord record = registeredUsers.get(username);
        if (record != null){record.numAuctionsSeller++;return true;}
        return false;
    }
//----------------------------------------------------------
//              SESSION METHODS
//----------------------------------------------------------
    /** Creates a new session, returns false if the session already exists*/
    public boolean addSession(String tokenId, String username, String ipAddress, int port) {
        if(!activeSessions.containsKey(tokenId)){
            activeSessions.put(tokenId, new SessionRecord(tokenId, ipAddress, port));
            System.out.println("[DataStore]> Session " + username + " has been registered successfully.");
            return true;
        }
        return false;
    }
    /** Checks if Session is recorded in activeSessions*/
    public boolean isSessionActive(String tokenId) {
        return activeSessions.containsKey(tokenId);
    }
    /** removes Session mapped with said tokenId from activeSessions*/
    public void removeSession(String tokenId) {
        SessionRecord removed = activeSessions.remove(tokenId);
        if (removed != null) {
            System.out.println("[DataStore] Session removed — token: " + tokenId + " | user: " + removed.username);
        }
    }
    /** Returns session based on tokenId*/
    public SessionRecord getSession(String tokenId) {
        return activeSessions.get(tokenId);
    }
    /** Returns username based on tokenId*/
    public String getUsernameByToken(String tokenId) {
        SessionRecord record = activeSessions.get(tokenId);
        return record != null ? record.username : null;
    }
    /** Returns true if user has an active session*/
    public boolean isUserLoggedIn(String username) {
        for (SessionRecord s : activeSessions.values()) {
            if (s.username.equals(username)) return true;
        }
        return false;
    }
//----------------------------------------------------------
//              AUCTION QUEUE METHODS
//----------------------------------------------------------

    /** Adds an item to the end off the auction queue*/
    public void enqueueItem(Item item) {
        auctionQueue.offer(item);
        System.out.println("[DataStore] Item enqueued: " + item.getObjectId()+" | Queue size: " + auctionQueue.size());
    }
    /** Removes and returns the first item of the queue*/
    public Item dequeueItem() throws InterruptedException {
        return auctionQueue.take();
    }
    /** Removes and returns the first item of the queue*/
    public int getQueueSize(){
        return auctionQueue.size();
    }



}
