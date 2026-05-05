package server;

import models.Item;
import models.Message;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class AuctionManagerThread extends Thread {
    private final DataStore dataStore;
    private final AuctionManager auctionManager;
    private final AuctionNotifier notifier;
    private final Item auctioningItem;
    private final String sellerToken;

    private final Set<String> activeBidders = Collections
            .newSetFromMap(new ConcurrentHashMap<>());
    // bid state
    private double highestBid;
    private String highestBidderToken;
    private final ReentrantLock bidLock = new ReentrantLock();
    // timer
    private volatile long auctionTimeLeft;
    private volatile boolean active;
    private volatile boolean cancelled = false;
    private String tag;

    public AuctionManagerThread(AuctionManager auctionManager, DataStore dataStore, Item item, String sellerToken) {
        this.auctionManager = auctionManager;
        this.dataStore = dataStore;
        this.notifier = new AuctionNotifier(dataStore);
        this.auctioningItem = item;
        this.active = true;
        this.auctionTimeLeft = item.getAuctionDuration();
        this.highestBid = item.getStartBid();
        this.highestBidderToken = null;
        this.sellerToken = sellerToken;
        this.tag = "[Auction:" + item.getObjectId() + "]";
    }

    // ----------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------

    public Item getAuctioningItem() {
        return auctioningItem;
    }

    public boolean isActive() {
        return active;
    }

    public double getHighestBid() {
        return highestBid;
    }

    public long getAuctionTimeLeft() {
        return auctionTimeLeft;
    }

    public String getSellerToken() {
        return sellerToken;
    }

    public Set<String> getActiveBidders() {
        return activeBidders;
    }

    // ----------------------------------------------------------------
    // Auction lifecycle
    // ----------------------------------------------------------------

    public void cancelAuction() {
        this.cancelled = true;
        this.active = false;
        this.interrupt();
    }

    public void startAuction() {
        System.out.println("\n" + tag + " Starting auction for item: " + auctioningItem.getObjectId()
                + " | Duration: " + auctioningItem.getAuctionDuration() + "s");
        start();
    }

    @Override
    public void run() {
        System.out.println(tag + " Started an auction");
        while (auctionTimeLeft > 0 && active) {
            try {
                Thread.sleep(1000);
                auctionTimeLeft--;
                if (auctionTimeLeft % 10 == 0 && auctionTimeLeft > 0) {
                    System.out.println(tag + " " + auctionTimeLeft + " seconds left for: "
                            + auctioningItem.getObjectId());
                }
            } catch (InterruptedException e) {
                System.err.println(tag + " Auction interrupted: " + auctioningItem.getObjectId());
                active = false;
            }
        }
        finalizeAuction();
    }

    public void finalizeAuction() {
        if (cancelled) {
            System.out.println("\n" + tag + " === AUCTION CANCELLED ===");
            return;
        }

        bidLock.lock();
        try {
            System.out.println("\n" + tag + " === AUCTION COMPLETE ===");

            boolean success = false;

            if (highestBidderToken != null) {
                // AUCTION WON
                String winnerUsername = dataStore.getUsernameByToken(highestBidderToken);
                DataStore.SessionRecord sellerSession = dataStore.getSession(sellerToken);

                if (sellerSession == null) {
                    System.out.println(tag + " Seller disconnected before auction ended.");
                } else if (winnerUsername == null) {
                    System.out.println(tag + " Winner disconnected before auction ended.");
                } else {
                    String sellerUsername = sellerSession.username;
                    String sellerp2pIp = sellerSession.p2pIpAddress;
                    Integer sellerp2pPort = sellerSession.p2pPort;

                    notifier.notifyAuctionWon(highestBidderToken, auctioningItem, highestBid, sellerUsername,
                            sellerp2pIp,
                            sellerp2pPort);
                    notifier.notifySellerSold(sellerToken, auctioningItem, highestBid, winnerUsername,
                            highestBidderToken);

                    dataStore.addSellerCount(sellerUsername);
                    dataStore.addBidderCount(winnerUsername);

                    System.out.println(tag + " Sold to: " + winnerUsername
                            + " | Final price: " + highestBid);
                    success = true;
                }
            }

            if (!success) {
                // AUCTION FAILED
                notifier.notifySellerNoBids(sellerToken, auctioningItem);
                System.out.println(tag + " AUCTION FAILED - No bids placed or participants disconnected.");

                if (highestBidderToken != null && dataStore.getSession(sellerToken) == null) {
                    auctionManager.notifyBiddersAuctionCancelled(activeBidders);
                }

                highestBidderToken = null; // Clear token so AuctionManager knows it failed
            }

            auctionManager.onAuctionComplete(auctioningItem, highestBidderToken, highestBid);

        } finally {
            bidLock.unlock();
        }
    }

    public Message placeBid(String tokenId, String objectId, double bidAmount) {
        if (!dataStore.isSessionActive(tokenId)) {
            return notifier.createErrorMessage("Invalid or expired session token.");
        }

        bidLock.lock();
        try {
            if (!auctioningItem.getObjectId().equals(objectId)) {
                return notifier.createErrorMessage(
                        "Bid does not match current item. Expected: " + auctioningItem.getObjectId());
            }

            if (bidAmount <= highestBid) {
                return notifier.createErrorMessage(
                        "Bid amount must be strictly higher than " + highestBid + ".");
            }

            this.highestBid = bidAmount;
            this.highestBidderToken = tokenId;
            activeBidders.add(tokenId);

            String bidderUsername = dataStore.getUsernameByToken(tokenId);
            System.out.println(tag + " New bid placed: " + bidAmount
                    + " by: " + bidderUsername);
        } finally {
            bidLock.unlock();
        }

        double capturedBid = highestBid;
        String capturedBidderToken = highestBidderToken;

        notifier.broadcastNewBid(objectId, capturedBid, tokenId);

        Message response = notifier.createSuccessMessage(
                "Bid placed successfully. Current highest bid: " + capturedBid);
        response.put("object_id", objectId);
        response.put("current_bid", capturedBid);
        response.put("highest_bidder_token", capturedBidderToken);
        return response;
    }
}
