package server;

import utils.Constants;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Main server entry point.
 * Opens ServerSocket and manages client connections using a Thread Pool.
 */
public class AuctionServer {
    private final DataStore dataStore;
    private final AccountManager accountManager;
    private final ExecutorService threadPool;

    public AuctionServer() {
        this.dataStore = new DataStore();
        this.accountManager = new AccountManager(dataStore);
        this.threadPool = Executors.newCachedThreadPool();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT)) {
            System.out.println("[AuctionServer]> Server started on port " + Constants.SERVER_PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("[AuctionServer]> New connection from: " + clientSocket.getInetAddress());

                // We pass accountManager and dataStore so the handler can process login/logout
                threadPool.execute(new ClientHandler(clientSocket, accountManager, dataStore));
            }
        } catch (IOException e) {
            System.err.println("[AuctionServer]> Server error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            threadPool.shutdown();
        }
    }

    public static void main(String[] args) {
        new AuctionServer().start();
    }
}
