package peer;

import models.Message;
import models.Message.MessageType;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class PeerApp {
    private static String tokenID = null;
    // private static String myIpAddr = null;
    // private static int myPort = 0;

    // UI to Initialize them
    private static String username = "";
    private static String password = "";

    private static PeerServer myPeerServer = null;
    private static AuctionClient myAuctionClient = null;
    private static ItemGenerator generator = null;

    public static void main(String[] args) {
        // initialize components
        myPeerServer = new PeerServer();
        myPeerServer.start();

        myAuctionClient = new AuctionClient();
        myAuctionClient.connect();

        // initialize random username and password
        username = generateRandom("user");
        password = generateRandom("pass");

        /*
         * while (myPeerServer.getListeningPort() == 0) {} // busy wait until port is
         * assigned
         * 
         * myPort = myPeerServer.getListeningPort();
         * myIpAddr = getMyIpAddress();
         */

        while (!register(username, password)) {
            // if someone already uses them, generate new credentials
            username = generateRandom("user");
            password = generateRandom("pass");
        }

        tokenID = login(username, password);
        myAuctionClient.setTokenID(tokenID);
        startGenerator();


        myAuctionClient.startPolling();

        // TODO here happens the actual running of the client in a loop
        // added a sleep for now
        try {
            System.out.println("[PeerApp]> App is running. Waiting for 2 minutes to test poller...");
            Thread.sleep(125000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopGenerator();
        myAuctionClient.stopPolling();
        logout(tokenID);
    }

    private static Boolean register(String username, String password) {
        Message reqLogIn = new Message(MessageType.REGISTER);
        reqLogIn.put("username", username);
        reqLogIn.put("password", password);

        myAuctionClient.sendMessage(reqLogIn);
        Message response = myAuctionClient.receiveMessage();

        if (response == null) {
            System.err.println("[PeerApp]> No response from server.");
            return false;
        }

        if (response.getType() != MessageType.SUCCESS) {
            System.err.println("[PeerApp]> Register failed: " + response.getString("message"));
            return false;
        }
        System.out.println("[PeerApp]> Register was successful");
        return true;
    }

    private static String login(String username, String password) {
        Message reqLogIn = new Message(MessageType.LOGIN);
        reqLogIn.put("username", username);
        reqLogIn.put("password", password);

        myAuctionClient.sendMessage(reqLogIn);
        Message response = myAuctionClient.receiveMessage();

        if (response == null) {
            System.err.println("[PeerApp]> No response from server.");
            return null;
        }

        if (response.getType() != MessageType.SUCCESS) {
            System.err.println("[PeerApp]> Login failed: " + response.getString("message"));
            return null;
        }

        return response.getString("token");
    }

    private static void logout(String token_id) {
        Message reqLogOut = new Message(MessageType.LOGOUT);
        reqLogOut.put("token", tokenID);

        myAuctionClient.sendMessage(reqLogOut);
        Message resp = myAuctionClient.receiveMessage();
        System.out.println("[PeerApp]> Sent Logout to Server: " + resp.getString("message"));
        System.out.println("[PeerApp]> Logging Out");

        myPeerServer.shutdown();
        myAuctionClient.disconnect();
    }

    /** Starts ItemGenerator */
    private static void startGenerator(){
        String sharedDirPath = "shared_directory_" + username; //unique shared directory name so multiple clients can exist in the same system
        generator = new ItemGenerator(myAuctionClient, username, sharedDirPath);
        generator.start();
    }
    /** Starts ItemGenerator */
    private static void stopGenerator(){
        generator.stopGenerator();
    }

    private static String generateRandom(String prefix) {
        return prefix + "_" + java.util.UUID.randomUUID().toString().substring(0, 8);
    }

    /*
     * private static String getMyIpAddress() {
     * try {
     * return InetAddress.getLocalHost().getHostAddress();
     * } catch (UnknownHostException e) {
     * System.err.println("[PeerApp]> Could not determine IP address: " +
     * e.getMessage());
     * return "127.0.0.1";
     * }
     * }
     */

}
