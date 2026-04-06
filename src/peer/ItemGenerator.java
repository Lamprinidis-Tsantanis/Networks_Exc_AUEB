package peer;

import models.Item;
import models.Message;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ItemGenerator extends Thread {
    private AuctionClient auctionClient;
    private String username;
    private String sharedDirectory;
    private boolean running = true;
    private int itemCount = 0;

    public ItemGenerator(AuctionClient auctionClient, String username, String sharedDirectory) {
        this.auctionClient = auctionClient;
        this.username = username;
        this.sharedDirectory = sharedDirectory;

        // creating the dir if it doesnt exist
        File dir = new File(sharedDirectory);
        if (!dir.exists()) {
            if (!dir.mkdirs()){
                System.err.println("Error creating directory " + sharedDirectory);
            }
        }
    }

    @Override
    public void run() {
        System.out.println("[ItemGenerator]> Started for user: " + username + " | Directory: " + sharedDirectory);
        while (running) {
            try {
                //calculating random time
                double rand = Math.random();
                long sleepTimeMs = (long) (rand * 120 * 1000);
                System.out.println("[ItemGenerator]> Next item will be generated in " + (sleepTimeMs / 1000) + " seconds.");
                Thread.sleep(sleepTimeMs);

                //create item
                String objectId = "Obj_" +  String.format("%02d", itemCount++) + username ;
                String description ="Item created by " + username + "_Obj_" + String.format("%02d ", itemCount++)+ " this is a short description";
                double startBid = (double)(10+(Math.random()*90.0)); //10-100 currency
                int duration = 30 + (int)(Math.random() * 60); //30-90 sec
                Item item = new Item(objectId,description, startBid, duration);

                //saves item to directory
                item.toFile(sharedDirectory);
                System.out.println("[ItemGenerator]> Created and saved new item: " + objectId);

                //notify server
                sendAuctionRequest(item);

            } catch (InterruptedException e) {
                System.out.println("[ItemGenerator]> Generator interrupted and stopped.");
            } catch (IOException e) {
                System.err.println("[ItemGenerator]> Error saving item file: " + e.getMessage());
            }
        }
    }

    /** Stops safely the thread when user logs out */
    public void stopGenerator() {
        running = false;
        this.interrupt();
    }

    /** Sends sendAuctionRequest to server with item */
    private void sendAuctionRequest(Item item) {
        Message req = new Message(Message.MessageType.REQUEST_AUCTION);
        List<Item> items = new ArrayList<>();
        items.add(item);
        req.put("items", items);

        Message response = auctionClient.sendAndReceive(req);

        if (response != null && response.getType() == Message.MessageType.SUCCESS) {
            System.out.println("[ItemGenerator]> Server successfully queued item: " + item.getObjectId());
        } else {
            System.err.println("[ItemGenerator]> Server failed to queue item: " + item.getObjectId());
        }
    }

}
