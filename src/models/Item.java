package models;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Item implements Serializable {
    private static final long serialVersionUID = 1L; // Σημαντικό για το ObjectOutputStream

    private String objectId;
    private String description;
    private double startBid;
    private int auctionDuration; // Σε δευτερόλεπτα

    public Item(String objectId, String description, double startBid, int auctionDuration) {
        this.objectId = objectId;
        this.description = description;
        this.startBid = startBid;
        this.auctionDuration = auctionDuration;
    }

    // --- Getters & Setters ---
    public String getObjectId() {
        return objectId;
    }

    public String getDescription() {
        return description;
    }

    public double getStartBid() {
        return startBid;
    }

    public  int getAuctionDuration() {
        return auctionDuration;
    }

    // --- Διαχείριση Αρχείων (T-02) ---

    // Γράφει το αντικείμενο σε αρχείο .txt
    public void toFile(String directoryPath) throws IOException {
        File file = new File(directoryPath, this.objectId + ".txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write("object_id:" + objectId + "\n");
            writer.write("description:" + description + "\n");
            writer.write("start_bid:" + startBid + "\n");
            writer.write("auction_duration:" + auctionDuration + "\n");
        }
    }

    // Διαβάζει ένα αρχείο .txt και δημιουργεί ένα Item
    public static Item fromFile(File file) throws IOException {
        Map<String, String> data = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":", 2);
                if (parts.length == 2) {
                    data.put(parts[0].trim(), parts[1].trim());
                }
            }
        }
        return new Item(
                data.get("object_id"),
                data.get("description"),
                Double.parseDouble(data.get("start_bid")),
                Integer.parseInt(data.get("auction_duration")));
    }

    @Override
    public String toString() {
        return "Item{" + "id='" + objectId + '\'' + ", desc='" + description + '\'' + ", bid=" + startBid + '}';
    }
}