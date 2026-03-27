package server;

import models.Item;

import java.util.List;

public class AuctionManager {
    private DataStore dataStore;
    private AuctionManagerThread currentAuctionThread;

    public AuctionManager(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * Validates the tokenId and adds items into the auctionQueue
     * Uses the enqueue of Datastore and saves it in its correct datastructure
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
    /** starts an auction by dequeueing the first item from the datastore, and by calling the auction manager thread*/
    public void startAuction() throws InterruptedException {
        try{
            Item nextItem = dataStore.dequeueItem();
            System.out.println("[AuctionManager]> Starting auction for Item: "+nextItem.getObjectId());
            currentAuctionThread = new AuctionManagerThread(dataStore,nextItem);
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
     *
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
     *         {@code null} if there is currently no active auction running.
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

    public void checkActive(String sellerTokenId) { // TODO wait for T011
        if (!dataStore.isSessionActive(sellerTokenId)) {
            dataStore.removeSession(sellerTokenId);
        }

    }
}
