package server;

import models.Item;
import models.Message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
/**
 * Class that manages the core functionalities of bidding.
 * It is only used by Auction Manager, but it is a separate file for
 * read-ability
 */
public class AuctionManagerThread extends Thread {
    private DataStore dataStore;
    private Item auctioningItem;
    private final ConcurrentHashMap <String, String> activeClients =  new ConcurrentHashMap<>();

    // bid state
    private double highestBid;
    private String highestBidderToken;
    private final ReentrantLock bidLock = new ReentrantLock();

    // timer
    private long auctionTimeLeft;// in seconds
    private boolean active;

    public AuctionManagerThread(DataStore dataStore, Item item) {
        this.dataStore = dataStore;
        this.auctioningItem = item;
        this.active = true;
        this.auctionTimeLeft = item.getAuctionDuration();
        this.highestBid = item.getStartBid();
        this.highestBidderToken = null;
    }

    public Item getAuctioningItem() {
        return auctioningItem;
    }

    public String getAuctioningItemDescription() {
        return auctioningItem.getDescription();
    }

    public long getAuctionTimeLeft() {
        return auctionTimeLeft;
    }

    public double getHighestBid() {
        return highestBid;
    }

    public boolean isActive() {
        return active;
    }

    public void startAuction() throws InterruptedException {
        Item auctioningItem = dataStore.dequeueItem(); // blocks automatically
        auctionTimeLeft = auctioningItem.getAuctionDuration();

        System.out.println("\n[AuctionManagerThread]> Starting auction for item: "+ auctioningItem.getObjectId() + " | Duration: " + auctioningItem.getAuctionDuration() + "s");
        start();
    }

    @Override
    public void run() {
        System.out.println("[AuctionManagerThread]> Started an auction");
        while (auctionTimeLeft > 0 && active) {
            try {
                Thread.sleep(1000);
                auctionTimeLeft--;
                if (auctionTimeLeft % 10 == 0 && auctionTimeLeft > 0) {
                    System.out.println("[AuctionManagerThread]> " + auctionTimeLeft +
                            " seconds left for: " + auctioningItem.getObjectId());
                }
            } catch (InterruptedException e) {
                System.err.println("[AuctionManagerThread]> Auction interrupted: " + auctioningItem.getObjectId() +"\n"+e.getMessage());
                active = false;
            }
        }
        finalizeAuction();
    }
    public double getCurrentHighestBid() {
        bidLock.lock();
        try {
            return highestBid;
        } finally {
            bidLock.unlock();
        }
    }

    public String getHighestBidderToken() {
        bidLock.lock();
        try {
            return highestBidderToken;
        } finally {
            bidLock.unlock();
        }
    }



    /** {@code needs implementation}*/
    public void finalizeAuction(){};

    /** {@code needs implementation}*/
    public Message placeBid(String tokenId, String objectId, double bidAmount) {
        // Validate session token
        if (!dataStore.isSessionActive(tokenId)) {
            return createErrorMessage("Invalid or expired session token.");
        }

        bidLock.lock();
        try {
            // Validate correct item
            if (!auctioningItem.getObjectId().equals(objectId)) {
                return createErrorMessage("Bid does not match current auctioning item. " +
                        "Expected: " + auctioningItem.getObjectId() +
                        ", Got: " + objectId);
            }

            // Validate bid amount is strictly higher than current highest
            if (bidAmount <= highestBid) {
                return createErrorMessage("Bid amount (" + bidAmount +
                        ") must be strictly higher than current highest bid (" +
                        highestBid + ").");
            }

            // Update highest bid and bidder
            this.highestBid = bidAmount;
            this.highestBidderToken = tokenId;

            // Get username of bidder
            String bidderUsername = dataStore.getUsernameByToken(tokenId);
            System.out.println("[AuctionManagerThread]> New bid placed: " + bidAmount +
                    " | Bidder: " + bidderUsername + " | Item: " + objectId);
        } finally {
            bidLock.unlock();
        }

        // Broadcast new bid to all active peers (outside lock to avoid deadlock)
        broadcastBidUpdate(tokenId, objectId, bidAmount);

        // Return success message with updated state
        Message response = createSuccessMessage("Bid placed successfully. " +
                "Current highest bid: " + highestBid);
        response.put("object_id", objectId);
        response.put("current_bid", highestBid);
        response.put("highest_bidder_token", highestBidderToken);
        return response;
    }

    private Message createSuccessMessage(String text) {
        Message msg = new Message(Message.MessageType.SUCCESS);
        msg.put("message", text);
        return msg;
    }

    private Message createErrorMessage(String text) {
        Message msg = new Message(Message.MessageType.ERROR);
        msg.put("message", text);
        return msg;
    }
    /** To be Implemented*/
    private void broadcastBidUpdate(String biddingTokenId, String objectId, double newBidAmount) {}
}
