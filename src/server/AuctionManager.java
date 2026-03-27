package server;
import models.Item;

import java.util.List;

public class AuctionManager {
    private DataStore dataStore;

    public AuctionManager(DataStore dataStore){
        this.dataStore=dataStore;
    }
    /**
     * Validates the tokenId and adds items into the auctionQueue
     * Uses the enqueue of Datastore and saves it in its correct datastructure
     * @param tokenId token of the user requesting auction.
     * @param itemList List of items that a user is putting in auction.
     * @return {@code true} if the tokenId is successfully validated and the list contains items<br>
     * {@code false} if the tokenId is not active or the list is empty or null<br><br><br>
     * {@code putting a lot of items may overload the datastore and provide no Error, only record in terminal by DataStore.enqueue }
     */
    public boolean getAuctionRequest(String tokenId, List<Item> itemList)
    {
        String error ="[AuctionManager]> ERROR while trying to add items to AuctionList: ";
        if (!dataStore.isSessionActive(tokenId)){System.out.println(error+"Invalid tokenId"); return false;}
        if (itemList==null || itemList.isEmpty()) {System.out.println(error+"Empty List"); return false;}
        for (Item item : itemList) {dataStore.enqueueItem(item,tokenId);} //here is a bit more complicated. putting a lot of items may overload the datastore and provide no Error
        System.out.println("[AuctionManager]> Received " + itemList.size() + " items from session " + tokenId);
        return true;
    }
    /** starts an auction by dequeueing the first item from the datastore, and by calling the auction manager thread*/
    public void startAuction() throws InterruptedException {
        try{
            Item nextItem = dataStore.dequeueItem();
            System.out.println("[AuctionManager]> Starting auction for Item: "+nextItem.getObjectId());
            AuctionManagerThread currentAuction = new AuctionManagerThread(dataStore,nextItem);
            currentAuction.startAuction();
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
}
