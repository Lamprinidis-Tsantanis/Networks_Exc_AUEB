package server;

import models.Item;
import models.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AuctionManager {
    private final DataStore dataStore;
    private final List<AuctionManagerThread> activeAuctions = new ArrayList<>();
    private static final int MAX_CONCURRENT_AUCTIONS = 2;

    public static class AuctionHistory {
        public final Item item;
        public final String sellerToken;
        public final ConcurrentHashMap<String, Double> bids;

        public AuctionHistory(Item item, String sellerToken, ConcurrentHashMap<String, Double> bids) {
            this.item = item;
            this.sellerToken = sellerToken;
            this.bids = bids;
        }
    }

    // map to store the history of each item
    private final ConcurrentHashMap<String, AuctionHistory> historyMap = new ConcurrentHashMap<>();

    public AuctionManager(DataStore dataStore) {
        this.dataStore = dataStore;
    }

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

    public void onAuctionComplete(Item item, String winnerToken, double finalBid, String sellerToken,
            ConcurrentHashMap<String, Double> bids) {
        System.out.println("[AuctionManager]> === AUCTION COMPLETE ===");
        System.out.println("[AuctionManager]> Item: " + item.getObjectId());
        System.out.println("[AuctionManager]> Winner: " +
                (winnerToken != null ? winnerToken : "No bids"));
        System.out.println("[AuctionManager]> Final price: " + finalBid);

        historyMap.put(item.getObjectId(), new AuctionHistory(item, sellerToken, bids));
    }

    /**
     * FALLBACK LOGIC
     * Handles the fallback logic when a winning bidder cancels their transaction.
     * <p>
     * This method retrieves the bid history for the specified item, removes the
     * bidder who canceled from the pool, and iterates through the remaining bids to
     * find the next highest offer. It verifies that the next candidate is currently
     * online before officially offering them the item.
     * <p>
     * If a valid, connected candidate is found, BOTH THE NEW BUYER AND THE SELLER
     * are notified of the updated transaction. If the seller is offline, or if the
     * list of bidders empties out with no active candidates, the fallback process
     * is safely aborted and the seller (if active) is notified of the failure.
     *
     * @param objectId       The unique identifier of the auctioned item.
     * @param cancelingToken The session token of the bidder who canceled the
     *                       transaction.
     */
    public void offerToNextBidder(String objectId, String cancelingToken) {
        AuctionHistory history = historyMap.get(objectId);
        if (history == null) {
            System.out.println("[AuctionManager]> No history found for item: " + objectId);
            return;
        }

        history.bids.remove(cancelingToken);

        AuctionNotifier notifier = new AuctionNotifier(dataStore);

        while (!history.bids.isEmpty()) {
            String nextToken = null;
            double maxBid = -1;

            for (java.util.Map.Entry<String, Double> entry : history.bids.entrySet()) {
                if (entry.getValue() > maxBid) {
                    maxBid = entry.getValue();
                    nextToken = entry.getKey();
                }
            }

            if (nextToken == null)
                break;

            if (dataStore.isSessionActive(nextToken)) {
                String newBuyerUsername = dataStore.getUsernameByToken(nextToken);
                DataStore.SessionRecord sellerSession = dataStore.getSession(history.sellerToken);

                if (sellerSession != null) {
                    String sellerUsername = sellerSession.username;
                    String sellerp2pIp = sellerSession.p2pIpAddress;
                    Integer sellerp2pPort = sellerSession.p2pPort;

                    notifier.notifyAuctionWon(nextToken, history.item, maxBid, sellerUsername, sellerp2pIp,
                            sellerp2pPort);
                    notifier.notifySellerSold(history.sellerToken, history.item, maxBid, newBuyerUsername, nextToken);

                    System.out.println("[AuctionManager]> Item " + objectId + " offered to next highest bidder: "
                            + newBuyerUsername + " for " + maxBid);
                    return;
                } else {
                    System.out.println("[AuctionManager]> Seller offline during fallback. Canceling fallback.");
                    return;
                }
            } else {
                history.bids.remove(nextToken);
            }
        }

        notifier.notifySellerNoBids(history.sellerToken, history.item);
        System.out.println("[AuctionManager]> No valid candidates left for item: " + objectId);
    }

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

    public void checkActive(String sellerTokenId) {
        DataStore.SessionRecord sellerSession = dataStore.getSession(sellerTokenId);

        if (sellerSession == null) {
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
