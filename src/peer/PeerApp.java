package peer;

import models.Message;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class PeerApp {
    private boolean firstRun=true;
    private String  tokenID = null;
    private String  myIpAddr = null;
    private int     myPort = null;
    //UI to Initialize them
    private String username = "";
    private String password = "";


    private PeerServer myPeerServer = null;
    private AuctionClient myAuctionClient = null;
    
    public static void main(String[] args) {

        this.myPeerServer = new PeerServer();
        this.myPeerServer.start();
        this.myAuctionClient = new AuctionClient();
        this.myAuctionClient.start();
        this.myAuctionClient.connect():
        // Here it must check more parameters
        if (firstRun){
            register(username,password)
        }
        tokenID = login()

        logout();
    }
    
    private static void   register(String username, String password){}
    //must send the server its own ip and port
    private static String login(String username, String password, String myIpAddr, int myPort){}
    private static void   logout(String token_id){}

}
