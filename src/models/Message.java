package models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    // Ορισμός των τύπων μηνυμάτων βάσει του T-03
    public enum MessageType {
        REGISTER, LOGIN, LOGOUT,
        REQUEST_AUCTION, GET_CURRENT_AUCTION, GET_AUCTION_DETAILS,
        PLACE_BID, AUCTION_RESULT, CHECK_ACTIVE,
        TRANSACTION,CONFIRM_OWNERSHIP,
        SUCCESS, ERROR // Βοηθητικά για απαντήσεις του Server
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

    // Προσθήκη δεδομένων στο μήνυμα (π.χ. message.put("username", "nikos"))
    public void put(String key, Object value) {
        payload.put(key, value);
    }

    // Ανάγνωση δεδομένων από το μήνυμα
    public Object get(String key) {
        return payload.get(key);
    }

    // Ανάγνωση ως String (βοηθητικό)
    public String getString(String key) {
        return (String) payload.get(key);
    }

    @Override
    public String toString() {
        return "Message{" + "type=" + type + ", payload=" + payload + '}';
    }
}