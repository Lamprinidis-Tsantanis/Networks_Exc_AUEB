package peer;

import models.Message;
import models.Message.MessageType;

public class PeerApp {
    private static String tokenID = null;
    private static String username = "";
    private static String password = "";
    private static String sharedDirPath;
    private static PeerServer myPeerServer = null;
    private static AuctionClient myAuctionClient = null;

    public static AuctionClient getAuctionClient() {
        return myAuctionClient;
    }

    private static ItemGenerator generator = null;

    public static void main(String[] args) {

        myPeerServer = new PeerServer();
        myPeerServer.start();

        sharedDirPath = "shared_directory_" + generateRandom("peer");
        myPeerServer.setDirectory(sharedDirPath);

        myAuctionClient = new AuctionClient();
        myAuctionClient.connect(sharedDirPath);

        // initialize random username and password
        username = generateRandom("user");
        password = generateRandom("pass");

        while (!register(username, password)) {
            // if someone already uses them, generate new credentials
            username = generateRandom("user");
            password = generateRandom("pass");
        }

        tokenID = login(username, password);

        System.setOut(new CustomPrintStream(System.out, username));
        System.setErr(new CustomPrintStream(System.err, username));

        myAuctionClient.setTokenID(tokenID);
        myAuctionClient.setUsername(username);
        startGenerator();

        myAuctionClient.startPolling();

        try {
            System.out.println("[PeerApp]> App is running. It will logout and stop in 5 minutes.");
            Thread.sleep(300000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopGenerator();
        myAuctionClient.stopPolling();
        logout(tokenID);
    }

    private static Boolean register(String username, String password) {
        Message reqRegister = new Message(MessageType.REGISTER);
        reqRegister.put("username", username);
        reqRegister.put("password", password);

        Message response = myAuctionClient.sendAndReceive(reqRegister);

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

        String myIp = getMyIpAddress();
        int myPort = myPeerServer.getListeningPort();

        reqLogIn.put("username", username);
        reqLogIn.put("password", password);
        reqLogIn.put("p2pIpAddress", myIp);
        reqLogIn.put("p2pPort", myPort);

        Message response = myAuctionClient.sendAndReceive(reqLogIn);

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

        Message resp = myAuctionClient.sendAndReceive(reqLogOut);
        System.out.println("[PeerApp]> Sent Logout to Server: " + resp.getString("message"));
        System.out.println("[PeerApp]> Logging Out");

        myPeerServer.shutdown();
        myAuctionClient.disconnect();
    }

    /** Starts ItemGenerator */
    private static void startGenerator() {

        myPeerServer.setDirectory(sharedDirPath);

        generator = new ItemGenerator(myAuctionClient, username, sharedDirPath);
        generator.start();
    }

    /** Starts ItemGenerator */
    private static void stopGenerator() {
        generator.stopGenerator();
    }

    private static String generateRandom(String prefix) {
        return prefix + "_" + java.util.UUID.randomUUID().toString().substring(0, 8);
    }

    private static String getMyIpAddress() {
        return "127.0.0.1";
    }
}
