package server;

import models.Item;
import models.Message;

/**
 * Helper class to manage the construction and broadcasting of auction messages.
 */
public class AuctionNotifier {
    private final DataStore dataStore;

    public AuctionNotifier(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public void notifyAuctionWon(String winnerToken, Item item, double finalPrice, String sellerUsername, String sellerp2pIp, Integer sellerp2pPort) {
        // SEND AUCTION_WON to the highest bidder
        Message auctionWonMsg = new Message(Message.MessageType.AUCTION_RESULT);
        auctionWonMsg.put("message", "Congratulations! You won the auction!");
        auctionWonMsg.put("status", "WON");
        auctionWonMsg.put("object_id", item.getObjectId());
        auctionWonMsg.put("object_description", item.getDescription());
        auctionWonMsg.put("final_price", finalPrice);
        auctionWonMsg.put("seller_username", sellerUsername);
        auctionWonMsg.put("p2pIpAddress", sellerp2pIp);
        auctionWonMsg.put("p2pPort", sellerp2pPort);
        auctionWonMsg.put("p2pPort", System.currentTimeMillis());

        broadcastMessageToClient(winnerToken, auctionWonMsg);
    }

    public void notifySellerSold(String sellerToken, Item item, double finalPrice, String buyerUsername, String buyerToken) {
        // SEND AUCTION_SOLD to seller
        Message auctionSoldMsg = new Message(Message.MessageType.AUCTION_RESULT);
        auctionSoldMsg.put("message", "Your item has been sold!");
        auctionSoldMsg.put("status", "SOLD");
        auctionSoldMsg.put("object_id", item.getObjectId());
        auctionSoldMsg.put("object_description", item.getDescription());
        auctionSoldMsg.put("final_price", finalPrice);
        auctionSoldMsg.put("buyer_username", buyerUsername);
        auctionSoldMsg.put("buyer_token", buyerToken);
        auctionSoldMsg.put("timestamp", System.currentTimeMillis());

        broadcastMessageToClient(sellerToken, auctionSoldMsg);
    }

    public void notifySellerNoBids(String sellerToken, Item item) {
        Message noWinnerMsg = new Message(Message.MessageType.AUCTION_RESULT);
        noWinnerMsg.put("message", "Auction ended with no bids. Item not sold.");
        noWinnerMsg.put("status", "NO_BIDS");
        noWinnerMsg.put("object_id", item.getObjectId());
        noWinnerMsg.put("object_description", item.getDescription());
        noWinnerMsg.put("timestamp", System.currentTimeMillis());

        broadcastMessageToClient(sellerToken, noWinnerMsg);
    }

    public void broadcastNewBid(String objectId, double newHighestBid, String bidderToSkipToken) {
        // Broadcast the new highest bid to all active clients
        Message updateMsg = new Message(Message.MessageType.AUCTION_RESULT);
        updateMsg.put("status", "NEW_HIGHEST_BID");
        updateMsg.put("message", "A new bid has been placed!");
        updateMsg.put("object_id", objectId);
        updateMsg.put("current_bid", newHighestBid);

        // Iterate through all active sessions in the DataStore and send them the update
        // We skip the person who just placed the bid, as they get the direct response
        // message
        for (String activeClientToken : dataStore.getActiveSessions().keySet()) {
            if (!activeClientToken.equals(bidderToSkipToken)) {
                broadcastMessageToClient(activeClientToken, updateMsg);
            }
        }
    }

    public Message createSuccessMessage(String text) {
        Message msg = new Message(Message.MessageType.SUCCESS);
        msg.put("message", text);
        return msg;
    }

    public Message createErrorMessage(String text) {
        Message msg = new Message(Message.MessageType.ERROR);
        msg.put("message", text);
        return msg;
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
                System.out.println("[AuctionNotifier]> Message sent to client: " + tokenId);
            } catch (Exception e) {
                System.err.println("[AuctionNotifier]> Failed to send message to " + tokenId +
                        ": " + e.getMessage());
            }
        } else {
            System.out.println("[AuctionNotifier]> Client not connected: " + tokenId);
        }
    }
}