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
    private String sellerToken;
    private final ConcurrentHashMap <String, String> activeClients =  new ConcurrentHashMap<>();

    // bid state
    private double highestBid;
    private String highestBidderToken;
    private final ReentrantLock bidLock = new ReentrantLock();

    // timer
    private long auctionTimeLeft;// in seconds
    private boolean active;

    public AuctionManagerThread(DataStore dataStore, Item item, String sellerToken) {
        this.dataStore = dataStore;
        this.auctioningItem = item;
        this.active = true;
        this.auctionTimeLeft = item.getAuctionDuration();
        this.highestBid = item.getStartBid();
        this.highestBidderToken = null;
        this.sellerToken = sellerToken;
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
    public void finalizeAuction(){
        bidLock.lock();
        try{
            System.out.println("\n[AuctionManagerThread]> === AUCTION COMPLETE ===");
            System.out.println("[AuctionManagerThread]> Item: " + auctioningItem.getObjectId());
            System.out.println("[AuctionManagerThread]> Winner: " +
                    (highestBidderToken != null ? dataStore.getUsernameByToken(highestBidderToken) : "No bids"));
            System.out.println("[AuctionManagerThread]> Final price: " + highestBid);

            // Remove item from auction queue mapping
            dataStore.removeItemFromAuction(auctioningItem.getObjectId());

            if (highestBidderToken != null) {
                // =============================================
                // AUCTION WON - Someone placed a bid
                // =============================================
                String winnerUsername = dataStore.getUsernameByToken(highestBidderToken);
                String sellerUsername = dataStore.getUsernameByToken(sellerToken);

                // SEND AUCTION_WON to highest bidder
                Message auctionWonMsg = new Message(Message.MessageType.AUCTION_RESULT);
                auctionWonMsg.put("message", "Congratulations! You won the auction!");
                auctionWonMsg.put("status", "WON");
                auctionWonMsg.put("object_id", auctioningItem.getObjectId());
                auctionWonMsg.put("object_description", auctioningItem.getDescription());
                auctionWonMsg.put("final_price", highestBid);
                auctionWonMsg.put("seller_username", sellerUsername);
                auctionWonMsg.put("timestamp", System.currentTimeMillis());

                broadcastMessageToClient(highestBidderToken, auctionWonMsg);

                System.out.println("[AuctionManagerThread]> AUCTION_WON sent to: " + winnerUsername +
                        " | Price: " + highestBid);

                // Increment bidder count for winner
                dataStore.addBidderCount(winnerUsername);

                // SEND AUCTION_SOLD to seller
                Message auctionSoldMsg = new Message(Message.MessageType.AUCTION_RESULT);
                auctionSoldMsg.put("message", "Your item has been sold!");
                auctionSoldMsg.put("status", "SOLD");
                auctionSoldMsg.put("object_id", auctioningItem.getObjectId());
                auctionSoldMsg.put("object_description", auctioningItem.getDescription());
                auctionSoldMsg.put("final_price", highestBid);
                auctionSoldMsg.put("buyer_username", winnerUsername);
                auctionSoldMsg.put("buyer_token", highestBidderToken);
                auctionSoldMsg.put("timestamp", System.currentTimeMillis());

                broadcastMessageToClient(sellerToken, auctionSoldMsg);

                System.out.println("[AuctionManagerThread]> AUCTION_SOLD sent to: " + sellerUsername +
                        " | Final Price: " + highestBid);

                // Increment seller count
                dataStore.addSellerCount(sellerUsername);

            } else {
                // =============================================
                // AUCTION FAILED - No bids placed
                // =============================================
                System.out.println("[AuctionManagerThread]> AUCTION FAILED - No bids placed");

                String sellerUsername = dataStore.getUsernameByToken(sellerToken);

                Message noWinnerMsg = new Message(Message.MessageType.AUCTION_RESULT);
                noWinnerMsg.put("message", "Auction ended with no bids. Item not sold.");
                noWinnerMsg.put("status", "NO_BIDS");
                noWinnerMsg.put("object_id", auctioningItem.getObjectId());
                noWinnerMsg.put("object_description", auctioningItem.getDescription());
                noWinnerMsg.put("timestamp", System.currentTimeMillis());

                broadcastMessageToClient(sellerToken, noWinnerMsg);

                System.out.println("[AuctionManagerThread]> AUCTION_FAILED sent to: " + sellerUsername);
            }

        } finally {
            bidLock.unlock();
        }
    }

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

        // Return success message with updated state
        Message response = createSuccessMessage("Bid placed successfully. " +
                "Current highest bid: " + highestBid);
        response.put("object_id", objectId);
        response.put("current_bid", highestBid);
        response.put("highest_bidder_token", highestBidderToken);
        return response;
    }

    /**
     * Broadcasts a message to a specific client (if they're connected)
     *
     * @param tokenId The token ID of the recipient
     * @param message The message to send
     */
    private void broadcastMessageToClient(String tokenId, Message message) {
        if (tokenId == null) return;

        // Check if client is in active clients map
        ClientHandler handler = dataStore.getClientHandler(tokenId);
        if (handler != null) {
            try {
                handler.sendMessage(message);
                System.out.println("[AuctionManagerThread]> Message sent to client: " + tokenId);
            } catch (Exception e) {
                System.err.println("[AuctionManagerThread]> Failed to send message to " + tokenId +
                        ": " + e.getMessage());
            }
        } else {
            System.out.println("[AuctionManagerThread]> Client not connected: " + tokenId);
        }
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
}
