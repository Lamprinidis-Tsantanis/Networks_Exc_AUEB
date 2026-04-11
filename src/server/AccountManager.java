package server;

import java.util.UUID;

public class AccountManager {
    private final DataStore dataStore;

    public AccountManager(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * Checks username uniqueness and registers a new user account.
     *
     * @param username The desired username.
     * @param password The desired password.
     * @return {@code true} if registration succeeded, false if the username is already taken.
     */
    public boolean register(String username, String password) {
        if (DataStore.userExists(username)) {return false;}
        return dataStore.registerUser(username, password);
    }

    /**
     * Verifies credentials, checks if already logged in, and creates a session.
     *
     * @return {@code token_id} if successful, {@code null} if authentication fails or user is
     *         already logged in.
     */
    public String login(String username, String password, String ipAddress, int port, String p2pIpAddress, int p2pPort) {
        if (DataStore.validateUser(username, password)) {

            if (DataStore.isUserLoggedIn(username)) {
                return null;
            }

            String tokenId = UUID.randomUUID().toString();
            boolean sessionAdded = DataStore.addSession(tokenId, username, ipAddress, port, p2pIpAddress, p2pPort);

            if (sessionAdded) {
                return tokenId;
            }
        }
        return null;
    }

    /**
     * Removes the active session associated with the provided token.
     *
     * @param tokenId The unique identifier of the session to terminate.
     */
    public void logout(String tokenId) {
        dataStore.removeSession(tokenId);
    }
}