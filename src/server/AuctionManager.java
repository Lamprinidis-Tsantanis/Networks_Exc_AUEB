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
     * Validates the tokenId and adds items into the auctionQueue
     * Uses the enqueue of Datastore and saves it in its correct data structure
     * 
     * @param tokenId  token of the user requesting auction.
     * @param itemList List of items that a user is putting in auction.
     * @return {@code true} if the tokenId is successfully validated and the list
     *         contains items<br>
     *         {@code false} if the tokenId is not active or the list is empty or
     *         null<br>
     *         <br>
     *         <br>
     *         {@code putting a lot of items may overload the datastore and provide no Error, only record in terminal by enqueue }
     *
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
        } // here is a bit more complicated. putting a lot of items may overload the
          // datastore and provide no Error
        System.out.println("[AuctionManager]> Received " + itemList.size() + " items from session " + tokenId);
        return true;
    }

    /**
     * starts an auction by de-queueing the first item from the datastore, and by
     * calling the auction manager thread
     */
    public void startAuction() throws InterruptedException {
        try {
            Item nextItem = dataStore.dequeueItem();
            String sellerToken = dataStore.getAndRemoveSellerToken(nextItem.getObjectId());

            System.out.println("[AuctionManager]> Starting auction for Item: " + nextItem.getObjectId());

            // Assign it to the class-level field 'currentAuctionThread', NOT a local
            // variable
            currentAuctionThread = new AuctionManagerThread(this, dataStore, nextItem, sellerToken);
            currentAuctionThread.startAuction();

        } catch (InterruptedException e) {
            System.err.println("[AuctionManager]> Interrupted: " + e.getMessage());
        }
    }

    public void onAuctionComplete(Item item, String winnerToken, double finalBid) throws InterruptedException {
        System.out.println("[AuctionManager]> === AUCTION COMPLETE ===");
        System.out.println("[AuctionManager]> Item: " + item.getObjectId());
        System.out.println("[AuctionManager]> Winner: " +
                (winnerToken != null ? winnerToken : "No bids"));
        System.out.println("[AuctionManager]> Final price: " + finalBid);

        // Start next auction if any
        startAuction();
    }

    /**
     * Retrieves the details of the currently active auction.
     * Checks the active auction thread to extract the objectId and description of
     * the item being auctioned.
     *
     * @return {@code String[]} containing exactly two elements: the objectId at
     *         index 0 and the description at index 1 <br>
     *         {@code null} if there is currently no active auction running<br>
     *         <br>
     *         <br>
     *         {@code This method relies on currentAuctionThread being correctly managed by startAuction and onAuctionComplete }
     */
    public String[] sendCurrentAuction() {
        if (currentAuctionThread != null) {
            Item activeItem = currentAuctionThread.getAuctioningItem();
            if (activeItem != null) {
                return new String[] { activeItem.getObjectId(), activeItem.getDescription() };
            }
        }

        return null;
    }

    /**
     * Retrieves the details of the currently active auction.
     * Queries the active auction thread and the datastore for current bid,
     * remaining time, and seller info.
     *
     * @return {@code Object[]} containing exactly three elements:<br>
     *         [0] -> seller tokenId (String)<br>
     *         [1] -> current highestBid (Double)<br>
     *         [2] -> remainingTime in seconds (Long)<br>
     *         {@code null} if there is currently no active auction running.<br>
     *         <br>
     *         <br>
     *         {@code Relies on currentAuctionThread being active and exposing its internal state.}
     */
    public Object[] sendAuctionDetails() {
        if (currentAuctionThread != null && currentAuctionThread.isActive()) {
            Item activeItem = currentAuctionThread.getAuctioningItem();

            if (activeItem != null) {
                String sellerToken = dataStore.getItemSeller(activeItem.getObjectId());
                double highestBid = currentAuctionThread.getHighestBid();
                long remainingTime = currentAuctionThread.getAuctionTimeLeft();

                return new Object[] { sellerToken, highestBid, remainingTime };
            }
        }

        // Return null if no auction is active
        return null;
    }

    /**
     * Detects if the seller's socket has dropped by attempting a connection
     * to their PeerServer.
     * If disconnected, it cancels the active auction, notifies all bidders, and
     * invalidates the token.
     *
     * @param sellerTokenId The token of the seller to check.
     */
    public void checkActive(String sellerTokenId) {
        DataStore.SessionRecord sellerSession = dataStore.getSession(sellerTokenId);
        if (sellerSession == null) {
            return;
        }

        boolean isAlive = true;
        try (java.net.Socket socket = new java.net.Socket(sellerSession.ipAddress, sellerSession.port);
                java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(socket.getOutputStream())) {

            // Ping the seller with a CHECK_ACTIVE message
            models.Message msg = new models.Message(models.Message.MessageType.CHECK_ACTIVE);
            out.writeObject(msg);
            out.flush();
        } catch (java.io.IOException e) {
            /*
             * Connection failed: Seller has dropped
             */
            isAlive = false;
        }

        if (!isAlive) {
            System.out.println("[AuctionManager]> Seller " + sellerTokenId + " disconnected. Cleaning up.");

            // Invalidate seller token
            dataStore.removeSession(sellerTokenId);

            // Cancel active auction if it belongs to this seller
            if (currentAuctionThread != null && currentAuctionThread.isActive()) {
                if (sellerTokenId.equals(currentAuctionThread.getSellerToken())) {
                    System.out.println("[AuctionManager]> Cancelling active auction due to seller disconnect.");

                    currentAuctionThread.cancelAuction();

                    // Notify all current bidders
                    notifyBiddersAuctionCancelled(currentAuctionThread.getActiveBidders());

                    // Clear the active thread state
                    currentAuctionThread = null;
                }
            }
        }
    }

    /**
     * Helper method to connect to each bidder's PeerServer and send an
     * AUCTION_CANCELLED notification.
     */
    private void notifyBiddersAuctionCancelled(java.util.Set<String> bidders) {
        if (bidders == null || bidders.isEmpty())
            return;

        for (String bidderToken : bidders) {
            DataStore.SessionRecord bidderSession = dataStore.getSession(bidderToken);
            if (bidderSession != null) {
                try (java.net.Socket socket = new java.net.Socket(bidderSession.ipAddress, bidderSession.port);
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
        if (currentAuctionThread != null && currentAuctionThread.isActive()) {
            return currentAuctionThread.placeBid(token, objectId, bidAmount);
        }
        Message msg = new Message(Message.MessageType.ERROR);
        msg.put("message", "No active auction right now.");
        return msg;
    }
}
