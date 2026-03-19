package server;

import java.util.UUID;

public class AccountManager {
    private final DataStore dataStore;

    public AccountManager(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * Verifies credentials, checks if already logged in, and creates a session.
     * @return token_id if successful, null if authentication fails or user is already logged in.
     */
    public String login(String username, String password, String ipAddress, int port) {
        if (DataStore.validateUser(username, password)) {

            if (DataStore.isUserLoggedIn(username)) {
                return null;
            }

            String tokenId = UUID.randomUUID().toString();
            boolean sessionAdded = DataStore.addSession(tokenId, username, ipAddress, port);

            if (sessionAdded) {
                return tokenId;
            }
        }

        return null;
    }

    /**
     * Removes the active session associated with the provided token.
     * @param tokenId The unique identifier of the session to terminate.
     */
    public void logout(String tokenId) {
        dataStore.removeSession(tokenId);
    }
}