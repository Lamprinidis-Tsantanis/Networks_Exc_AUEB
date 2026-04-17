package peer;

import java.io.PrintStream;

/**
 * Wraps System.out to automatically prepend username to all output.
 * Helps identify which peer is producing logs in multi-peer scenarios.
 */
public class CustomPrintStream extends PrintStream {

    private final String username;
    private final PrintStream original;

    public CustomPrintStream(PrintStream original, String username) {
        super(original);
        this.original = original;
        this.username = username;
    }

    @Override
    public void println(String x) {
        if (x != null && !x.isEmpty()) {
            original.println("[" + username + "] " + x);
        } else {
            original.println();
        }
        original.flush();
    }

    @Override
    public void print(String x) {
        if (x != null && !x.isEmpty()) {
            original.print("[" + username + "] " + x);
        }
        original.flush();
    }

    @Override
    public PrintStream printf(String format, Object... args) {
        original.printf("[" + username + "] " + format, args);
        original.flush();
        return null;
    }
}