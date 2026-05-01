package server;

import models.Item;
import models.Message;
import java.util.List;

public class AuctionManager {
    private final DataStore dataStore;
    private AuctionManagerThread currentAuctionThread;

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
    public void startAuction() throws InterruptedException {
        while (true) {
            DataStore.AuctionEntry nextAuctionEntry = dataStore.dequeueItem();
            Item nextItem = nextAuctionEntry.auctionItem;
            String sellerToken = nextAuctionEntry.sellerTokenId;

            if (!dataStore.isSessionActive(sellerToken)) {
                System.out.println(
                        "[AuctionManager]> Skipping item " + nextItem.getObjectId() + " because seller disconnected.");
                continue;
            }

            System.out.println("[AuctionManager]> Starting auction for Item: " + nextItem.getObjectId());

            currentAuctionThread = new AuctionManagerThread(this, dataStore, nextItem, sellerToken);
            currentAuctionThread.startAuction();
            currentAuctionThread.join();
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
     * Retrieves the object ID and description of the currently active auction.
     * Also triggers a seller liveness check before returning data.
     *
     * @return {@code String[]} with objectId at [0] and description at [1],
     *         or {@code null} if no auction is active.
     */
    public String[] sendCurrentAuction() {
        AuctionManagerThread activeThread = currentAuctionThread;
        if (activeThread != null && activeThread.isActive()) {
            checkActive(activeThread.getSellerToken());

            activeThread = currentAuctionThread;
            if (activeThread != null && activeThread.isActive()) {
                Item activeItem = activeThread.getAuctioningItem();
                if (activeItem != null) {
                    return new String[] { activeItem.getObjectId(), activeItem.getDescription() };
                }
            }
        }
        return null;
    }

    /**
     * Retrieves details of the currently active auction.
     * Also triggers a seller liveness check before returning data.
     *
     * @return {@code Object[]} with sellerTokenId at [0], highestBid at [1],
     *         and remainingTime in seconds at [2], or {@code null} if no auction is
     *         active.
     */
    public Object[] sendAuctionDetails() {
        AuctionManagerThread activeThread = currentAuctionThread;
        if (activeThread != null && activeThread.isActive()) {
            checkActive(activeThread.getSellerToken());

            activeThread = currentAuctionThread;
            if (activeThread != null && activeThread.isActive()) {
                String sellerToken = activeThread.getSellerToken();
                double highestBid = activeThread.getHighestBid();
                long remainingTime = activeThread.getAuctionTimeLeft();
                long totalDuration = activeThread.getAuctioningItem().getAuctionDuration();
                return new Object[] { sellerToken, highestBid, remainingTime, totalDuration };
            }
        }
        return null;
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
            // Seller logged out gracefully or abruptly disconnected.
            synchronized (this) {
                AuctionManagerThread activeThread = currentAuctionThread;
                if (activeThread != null && activeThread.isActive()) {
                    if (sellerTokenId.equals(activeThread.getSellerToken())) {
                        System.out.println("[AuctionManager]> Cancelling active auction due to seller logout.");
                        activeThread.cancelAuction();
                        notifyBiddersAuctionCancelled(activeThread.getActiveBidders());
                        currentAuctionThread = null;
                    }
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
                AuctionManagerThread activeThread = currentAuctionThread;
                if (activeThread != null && activeThread.isActive()) {
                    if (sellerTokenId.equals(activeThread.getSellerToken())) {
                        System.out.println("[AuctionManager]> Cancelling active auction due to seller disconnect.");

                        activeThread.cancelAuction();
                        notifyBiddersAuctionCancelled(activeThread.getActiveBidders());
                        currentAuctionThread = null;
                    }
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
        AuctionManagerThread activeThread = currentAuctionThread;
        if (activeThread != null && activeThread.isActive()) {
            return activeThread.placeBid(token, objectId, bidAmount);
        }
        Message msg = new Message(Message.MessageType.ERROR);
        msg.put("message", "No active auction right now.");
        return msg;
    }
}
