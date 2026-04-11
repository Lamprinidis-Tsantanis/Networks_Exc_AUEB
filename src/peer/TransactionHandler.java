package peer;

import models.Message;
import models.Message.MessageType;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TransactionHandler implements Runnable {
    private String objectId ;
    private String objectDesc ;
    private double finalPrice ;

    private String sellerTransIp;
    private int sellerTransPort;

    private  String sharedDir;
    private AuctionClient auctionClient;

    private static int CONNECTION_TIMEOUT = 5000;

    public TransactionHandler(String sellerIp, int sellerPort, String objectId, double winningBid, String sharedDirectory, AuctionClient auctionClient) {
        this.sellerTransIp        = sellerIp;
        this.sellerTransPort      = sellerPort;
        this.objectId        = objectId;
        this.finalPrice      = winningBid;
        this.sharedDir = sharedDirectory;
        this.auctionClient   = auctionClient;
    }


    @Override
    public void run() {
        System.out.println("[TransactionHandler]> Starting transaction for item: "
                + objectId + " from seller " + sellerTransIp + ":" + sellerTransPort);
        try {
            byte[] fileBytes = fetchFileFromServer();
            saveFileToDisk(fileBytes);
            notifyServerOfOwnership();
        } catch (TransactionException e){
            System.err.println("[TransactionHandler]> Transaction failed: " + e.getMessage());
        }


    }





    private byte[] fetchFileFromServer() throws TransactionException{
        try(Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(sellerTransIp, sellerTransPort), CONNECTION_TIMEOUT);

            // set in/out
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            // send request to server for the transaction
            Message confirm = new Message(Message.MessageType.TRANSACTION);
            confirm.put("object_id",   objectId);
            confirm.put("winning_bid", String.valueOf(finalPrice));
            out.writeObject(confirm);
            out.flush();
            out.reset();

            // read response
            Message response = (Message) in.readObject();
            if (response == null) {
                throw new TransactionException("Seller returned null response.");
            }
            if (response.getType() != MessageType.SUCCESS) {
                String reason = response.getString("message");
                throw new TransactionException(
                        "Seller rejected transfer: " + (reason != null ? reason : "unknown reason"));
            }
            byte[] fileBytes = (byte[]) in.readObject();

            if (fileBytes == null || fileBytes.length == 0) {
                throw new TransactionException("Seller sent empty file for item: " + objectId);
            }

            System.out.println("[TransactionHandler]> Received " + fileBytes.length
                    + " bytes from seller for item: " + objectId);
            return fileBytes;

        } catch (java.net.SocketTimeoutException e) {
            throw new TransactionException(
                    "Timed out connecting to seller at " + sellerTransIp + ":" + sellerTransPort);
        } catch (IOException e) {
            throw new TransactionException(
                    "I/O error during seller connection: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new TransactionException(
                    "Received an unrecognised object from seller: " + e.getMessage());
        }
    }
    private void saveFileToDisk(byte[] fileBytes) throws TransactionException {
        try{
            Path dirPath = Paths.get(sharedDir);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }
            Path filePath = dirPath.resolve(objectId + ".txt");
            Files.write(filePath, fileBytes);
            System.out.println("[TransactionHandler]> File saved to: " + filePath.toAbsolutePath());
        }catch (IOException e) {
            throw new TransactionException("Failed to save file: " + e.getMessage());
        }
    }
    private void notifyServerOfOwnership() throws TransactionException {
        Message notify = new Message(MessageType.CONFIRM_OWNERSHIP);
        notify.put("object_id",   objectId);
        notify.put("winning_bid", String.valueOf(finalPrice));

        Message response = auctionClient.sendAndReceive(notify);

        if (response == null) {
            throw new TransactionException(
                    "No response from AuctionServer during ownership notification.");
        }

        if (response.getType() != MessageType.SUCCESS) {
            String reason = response.getString("message");
            throw new TransactionException(
                    "AuctionServer rejected ownership notification: "
                            + (reason != null ? reason : "unknown reason"));
        }

        System.out.println("[TransactionHandler]> AuctionServer confirmed new ownership of: " + objectId);

    }

    public static class TransactionException extends Exception {
        public TransactionException(String message) {
            super(message);
        }
    }
}
