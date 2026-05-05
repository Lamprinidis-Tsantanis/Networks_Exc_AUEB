package models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum MessageType {
        REGISTER, LOGIN, LOGOUT,
        REQUEST_AUCTION, GET_CURRENT_AUCTION, GET_AUCTION_DETAILS,
        PLACE_BID, AUCTION_RESULT, CHECK_ACTIVE,
        TRANSACTION, CONFIRM_OWNERSHIP, CANCEL_TRANSACTION,
        SUCCESS, ERROR
    }

    private MessageType type;
    private Map<String, Object> payload;

    public Message(MessageType type) {
        this.type = type;
        this.payload = new HashMap<>();
    }

    public MessageType getType() {
        return type;
    }

    public void put(String key, Object value) {
        payload.put(key, value);
    }

    public Object get(String key) {
        return payload.get(key);
    }

    public String getString(String key) {
        return (String) payload.get(key);
    }

    @Override
    public String toString() {
        return "Message{" + "type=" + type + ", payload=" + payload + '}';
    }
}
