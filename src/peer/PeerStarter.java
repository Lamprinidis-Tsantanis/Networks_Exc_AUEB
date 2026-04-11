package peer;

import java.io.*;
import java.util.*;

public class PeerStarter {

    private static final int DEFAULT_PEER_COUNT = 5;
    private static final int COUNTDOWN_SECONDS = 10;

    public static void main(String[] args) {
        int desired = askHowMany();
        List<Process> processes = spawnPeers(desired);
        registerShutdownHook(processes);
        waitAndShutdown(processes);
    }

    /**
     * Asks user how many peers to spawn. If no input within timeout, defaults to 5
     *
     * @return the number of peers to create
     */
    private static int askHowMany() {
        System.out.printf("%n[PEER_STARTER] How many peers to start? [auto-starting %d in %ds]: ",
                DEFAULT_PEER_COUNT, COUNTDOWN_SECONDS);

        try {
            long deadline = System.currentTimeMillis() + COUNTDOWN_SECONDS * 1000L;
            while (System.currentTimeMillis() < deadline) {
                if (System.in.available() > 0) {
                    String input = new Scanner(System.in).nextLine().trim();
                    int parsed = Integer.parseInt(input);
                    if (parsed >= 1) {
                        System.out.printf("[PEER_STARTER] Starting %d peers.%n", parsed);
                        return parsed;
                    }
                    System.out.printf("[PEER_STARTER] Invalid input, starting default %d.%n", DEFAULT_PEER_COUNT);
                    return DEFAULT_PEER_COUNT;
                }
                Thread.sleep(100);
            }
        } catch (NumberFormatException e) {
            System.out.printf("%n[PEER_STARTER] Invalid input, starting default %d.%n", DEFAULT_PEER_COUNT);
        } catch (InterruptedException | IOException ignored) {
        }

        System.out.printf("%n[PEER_STARTER] Time's up, starting default %d.%n", DEFAULT_PEER_COUNT);
        return DEFAULT_PEER_COUNT;
    }

    /**
     * Spawns multiple peer instances
     *
     * @param count the number of peers to spawn
     * @return the list of processes that were created
     */
    private static List<Process> spawnPeers(int count) {
        String javaCmd = ProcessHandle.current().info().command().orElse("java");
        String classpath = System.getProperty("java.class.path");

        List<Process> processes = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            try {
                ProcessBuilder pb = new ProcessBuilder(javaCmd, "-cp", classpath, "peer.PeerApp");
                pb.inheritIO();
                Process p = pb.start();
                processes.add(p);
                System.out.printf("[PEER_STARTER] Peer #%d started (PID %d)%n", i, p.pid());

                // Small delay between peer spawns to avoid port conflicts
                Thread.sleep(500);
            } catch (IOException e) {
                System.err.printf("[PEER_STARTER] Failed to start peer #%d: %s%n", i, e.getMessage());
            } catch (InterruptedException e) {
                System.err.println("[PEER_STARTER] Spawn interrupted: " + e.getMessage());
            }
        }
        return processes;
    }

    /**
     * Adds shutdown hooks to the runtime, so that when this thread shuts down, so do the peers
     *
     * @param processes the list of processes that will be added to the runtime
     */
    private static void registerShutdownHook(List<Process> processes) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[PEER_STARTER] Shutdown detected, killing all peers...");
            for (int i = 0; i < processes.size(); i++) {
                Process p = processes.get(i);
                if (p.isAlive()) {
                    p.destroyForcibly();
                    System.out.printf("  Peer #%d terminated%n", i + 1);
                }
            }
            System.out.println("[PEER_STARTER] All peers terminated.");
        }, "ShutdownHook"));
    }

    /**
     * Waits for user to press enter, and kills all peers and self.
     *
     * @param processes the processes that will be deleted if user presses enter
     */
    private static void waitAndShutdown(List<Process> processes) {
        System.out.println("\n[PEER_STARTER] All peers launched. Press ENTER anytime to shut them all down.\n");
        new Scanner(System.in).nextLine();

        System.out.println("[PEER_STARTER] Shutting down peers...");
        for (int i = 0; i < processes.size(); i++) {
            Process p = processes.get(i);
            if (p.isAlive()) {
                p.destroyForcibly();
                System.out.printf("  Peer #%d killed%n", i + 1);
            }
        }
        System.out.println("[PEER_STARTER] Done.");
    }
}