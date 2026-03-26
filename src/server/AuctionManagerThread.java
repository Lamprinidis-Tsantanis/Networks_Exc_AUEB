package server;

import models.Item;

/**
 * Class that manages the core functionalities of bidding.
 * It is only used by Auction Manager, but it is a separate file for read-ability
 */
public class AuctionManagerThread extends Thread {
    private DataStore dataStore;
    private Item auctioningItem;
    private long auctionTimeLeft;//in seconds
    private boolean active;

    public AuctionManagerThread(DataStore dataStore,Item item){
        this.dataStore=dataStore;
        this.auctioningItem=item;
        this.active=true;
    }

    public Item getAuctioningItem() {return auctioningItem;}
    public String getAuctioningItemDescription(){return auctioningItem.getDescription();}

    public void startAuction() throws InterruptedException {
        Item auctioningItem =  dataStore.dequeueItem(); //blocks automatically
        auctionTimeLeft = auctioningItem.getAuctionDuration();

        System.out.println("\n[AuctionManagerThread]> Starting auction for item: "+ auctioningItem.getObjectId() + " | Duration: " + auctioningItem.getAuctionDuration() + "s");
        start();
    }

    @Override
    public void run(){
        System.out.println("[AuctionManagerThread]> Started an auction");
        while(auctionTimeLeft>0 && active){
            try{
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
    public void finalizeAuction(){};
    public boolean placeBid(){return false;}
}
