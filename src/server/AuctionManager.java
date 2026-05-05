package server;

import models.Item;
import models.Message;

import java.util.ArrayList;
import java.util.List;

public class AuctionManager {
    private final DataStore dataStore;
    private final List<AuctionManagerThread> activeAuctions = new ArrayList<>();
    private static final int MAX_CONCURRENT_AUCTIONS = 2;

    public AuctionManager(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * Validates the tokenId and adds items into the auctionQueue.
     *
     * @param tokenId  token of the user requesting auction.
     * @param itemList List of items that a user is putting in auction.
     * @return {@code true} if the tokenId is successfully validated and the list
     *         contains items, {@code false} otherwise.
     */
    public boolean getAuctionRequest(String tokenId, List<Item> itemList) {
        String error = "[AuctionManager]> ERROR while trying to add items to AuctionList: ";
        if (!dataStore.isSessionActive(tokenId)) {
            System.out.println(error + "Invalid tokenId");
            return false;
        }
        if (itemList == null || itemList.isEmpty()) {
            System.out.println(error + "Empty List");
            return false;
        }
        for (Item item : itemList) {
            dataStore.enqueueItem(item, tokenId);
        }
        System.out.println("[AuctionManager]> Received " + itemList.size() + " items from session " + tokenId);
        return true;
    }

    /**
     * Continuously dequeues items and runs one auction at a time.
     * Blocks on dequeueItem() when the queue is empty, and waits for the
     * auction thread to finish before starting the next one.
     */
    // TODO one at a time ?
    public void startAuction() throws InterruptedException {
        while (true) {
            activeAuctions.removeIf(thread -> !thread.isActive());

            while (activeAuctions.size() >= MAX_CONCURRENT_AUCTIONS) {
                Thread.sleep(1000);
                activeAuctions.removeIf(thread -> !thread.isActive());
            }

            DataStore.AuctionEntry nextAuctionEntry = dataStore.dequeueItem();
            Item nextItem = nextAuctionEntry.auctionItem;
            String sellerToken = nextAuctionEntry.sellerTokenId;

            if (!dataStore.isSessionActive(sellerToken)) {
                System.out.println(
                        "[AuctionManager]> Skipping item " + nextItem.getObjectId() + " because seller disconnected.");
                continue;
            }

            System.out.println("[AuctionManager]> Starting auction for Item: " + nextItem.getObjectId());

            AuctionManagerThread newAuction = new AuctionManagerThread(this, dataStore, nextItem, sellerToken);
            activeAuctions.add(newAuction);
            newAuction.startAuction();
        }
    }

    public void onAuctionComplete(Item item, String winnerToken, double finalBid) {
        System.out.println("[AuctionManager]> === AUCTION COMPLETE ===");
        System.out.println("[AuctionManager]> Item: " + item.getObjectId());
        System.out.println("[AuctionManager]> Winner: " +
                (winnerToken != null ? winnerToken : "No bids"));
        System.out.println("[AuctionManager]> Final price: " + finalBid);
    }

    /**
     * Retrieves the object ID and description of the currently active auctions.
     * Also triggers a seller liveness check before returning data.
     *
     * @return {@code List <String[]>} where for each item, objectId at [0] and
     *         description at [1],
     *         or {@code empty List} if no auction is active.
     */
    public List<String[]> getAllCurrentAuctions() {
        activeAuctions.removeIf(thread -> !thread.isActive());
        List<String[]> result = new ArrayList<>();

        for (AuctionManagerThread auction : activeAuctions) {
            checkActive(auction.getSellerToken());

            if (auction.isActive()) {
                Item item = auction.getAuctioningItem();
                result.add(new String[] { item.getObjectId(), item.getDescription() });
            }
        }
        return result;
    }

    /**
     * Retrieves details of the currently active auction.
     * Also triggers a seller liveness check before returning data.
     *
     * @return {@code List<Object[]>} where each List item has sellerTokenId at [0],
     *         highestBid at [1],
     *         remainingTime in seconds at [2], startingTime of the auction [3]
     *         and the objectID at [4]
     *         or {@code null} if no auction is
     *         active.
     */
    public List<Object[]> getAllAuctionDetails() {
        activeAuctions.removeIf(thread -> !thread.isActive());
        List<Object[]> result = new ArrayList<>();

        for (AuctionManagerThread auction : activeAuctions) {
            checkActive(auction.getSellerToken());

            if (auction.isActive()) {
                result.add(new Object[] {
                        auction.getSellerToken(),
                        auction.getHighestBid(),
                        auction.getAuctionTimeLeft(),
                        auction.getAuctioningItem().getAuctionDuration(),
                        auction.getAuctioningItem().getObjectId(),
                        dataStore.getReputation(dataStore.getUsernameByToken(auction.getSellerToken()))
                });
            }
        }
        return result;
    }

    /**
     * Checks whether the seller identified by sellerTokenId is still reachable
     * by attempting a connection to their PeerServer p2p port.
     * If the seller has disconnected, the active auction is cancelled and all
     * current bidders are notified.
     *
     * @param sellerTokenId The token of the seller to check.
     */
    public void checkActive(String sellerTokenId) {
        DataStore.SessionRecord sellerSession = dataStore.getSession(sellerTokenId);

        if (sellerSession == null) {
            // Seller logged out - cancel ALL their auctions
            System.out.println(
                    "[AuctionManager]> Seller " + sellerTokenId + " logged out. Cancelling all their auctions.");

            synchronized (this) {
                List<AuctionManagerThread> sellerAuctions = getSellerAuctions(sellerTokenId);
                for (AuctionManagerThread auction : sellerAuctions) {
                    auction.cancelAuction();
                    notifyBiddersAuctionCancelled(auction.getActiveBidders());
                    activeAuctions.remove(auction);
                }
            }
            return;
        }
        // Check if seller is still reachable via network
        boolean isAlive = true;
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(sellerSession.p2pIpAddress, sellerSession.p2pPort), 2000);
            try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(socket.getOutputStream())) {
                models.Message msg = new models.Message(models.Message.MessageType.CHECK_ACTIVE);
                out.writeObject(msg);
                out.flush();
            }
        } catch (java.io.IOException e) {
            isAlive = false;
        }
        if (!isAlive) {
            System.out.println("[AuctionManager]> Seller " + sellerTokenId + " disconnected. Cleaning up.");
            dataStore.removeSession(sellerTokenId);

            synchronized (this) {
                List<AuctionManagerThread> sellerAuctions = getSellerAuctions(sellerTokenId);
                for (AuctionManagerThread auction : sellerAuctions) {
                    auction.cancelAuction();
                    notifyBiddersAuctionCancelled(auction.getActiveBidders());
                    activeAuctions.remove(auction);
                }
            }
        }
    }

    /**
     * Sends an AUCTION_CANCELLED notification to each bidder's PeerServer p2p port.
     */
    public void notifyBiddersAuctionCancelled(java.util.Set<String> bidders) {
        if (bidders == null || bidders.isEmpty())
            return;

        for (String bidderToken : bidders) {
            DataStore.SessionRecord bidderSession = dataStore.getSession(bidderToken);
            if (bidderSession != null) {
                try (java.net.Socket socket = new java.net.Socket(bidderSession.p2pIpAddress, bidderSession.p2pPort);
                        java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(socket.getOutputStream())) {

                    models.Message msg = new models.Message(models.Message.MessageType.AUCTION_RESULT);
                    msg.put("status", "AUCTION_CANCELLED");
                    msg.put("message", "The seller disconnected. The auction has been cancelled.");

                    out.writeObject(msg);
                    out.flush();
                } catch (java.io.IOException e) {
                    System.err
                            .println("[AuctionManager]> Failed to notify bidder " + bidderToken + " of cancellation.");
                }
            }
        }
    }

    public Message placeBid(String token, String objectId, double bidAmount) {
        AuctionManagerThread activeThread = getAuctionThreadByObject(objectId);
        if (activeThread != null && activeThread.isActive()) {
            return activeThread.placeBid(token, objectId, bidAmount);
        }
        Message msg = new Message(Message.MessageType.ERROR);
        msg.put("message", "No active auction right now.");
        return msg;
    }

    private AuctionManagerThread getAuctionThreadByObject(String objectId) {
        for (AuctionManagerThread auction : activeAuctions) {
            if (auction.isActive() &&
                    auction.getAuctioningItem().getObjectId().equals(objectId)) {
                return auction;
            }
        }
        return null;
    }

    private List<AuctionManagerThread> getSellerAuctions(String sellerTokenId) {
        List<AuctionManagerThread> sellerAuctions = new ArrayList<>();
        for (AuctionManagerThread auction : activeAuctions) {
            if (auction.isActive() && sellerTokenId.equals(auction.getSellerToken())) {
                sellerAuctions.add(auction);
            }
        }
        return sellerAuctions;
    }
}
