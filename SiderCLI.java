import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Scanner;

public class SiderCLI {

    // Configuration
    private static final String DEFAULT_HOST = "20.197.19.241";
    private static final int DEFAULT_PORT = 4000;

    // ==========================================
    // SIDER CLIENT (DRIVER)
    // ==========================================
    public static class SiderClient implements AutoCloseable {
        private String host;
        private int port;
        private Socket socket;
        private PrintWriter out;
        private BufferedReader in;

        public SiderClient(String host, int port) {
            this.host = host;
            this.port = port;
            connect();
        }

        public boolean connect() {
            try {
                socket = new Socket();
                // 5 second timeout for connection and reads
                socket.connect(new InetSocketAddress(host, port), 5000);
                socket.setSoTimeout(5000);
                
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                return true;
            } catch (IOException e) {
                close();
                return false;
            }
        }

        public boolean isConnected() {
            return socket != null && !socket.isClosed() && socket.isConnected();
        }

        private String sendCommand(String command) {
            if (!isConnected()) {
                // Try to reconnect once
                if (!connect()) {
                    return "Error: Not connected to server.";
                }
            }

            try {
                // Protocol: Command must end with newline
                out.println(command);
                
                String response = in.readLine();
                if (response == null) {
                    close();
                    return "Error: Server closed connection.";
                }
                return response;
            } catch (SocketTimeoutException e) {
                return "Error: Request timed out.";
            } catch (IOException e) {
                close();
                return "Error: Connection lost (" + e.getMessage() + ")";
            }
        }

        public String put(String key, String value) {
            return sendCommand("PUT " + key + " " + value);
        }

        public String get(String key) {
            return sendCommand("GET " + key);
        }

        public String delete(String key) {
            return sendCommand("DEL " + key);
        }

        public String compact() {
            return sendCommand("COMPACT");
        }
        
        // Pass raw string directly (helper for CLI)
        public String sendRaw(String rawCmd) {
            return sendCommand(rawCmd);
        }

        @Override
        public void close() {
            try {
                if (socket != null) socket.close();
                if (in != null) in.close();
                if (out != null) out.close();
            } catch (IOException e) {
                // Ignore errors during close
            } finally {
                socket = null;
            }
        }
    }

    // ==========================================
    // INTERACTIVE CLI (MAIN)
    // ==========================================
    public static void main(String[] args) {
        String host = (args.length > 0) ? args[0] : DEFAULT_HOST;
        int port = (args.length > 1) ? Integer.parseInt(args[1]) : DEFAULT_PORT;

        System.out.println("🔌 Connecting to Sider at " + host + ":" + port + "...");
        
        SiderClient client = new SiderClient(host, port);

        if (client.isConnected()) {
            System.out.println("✅ Connected! (Host: " + host + ")");
        } else {
            System.out.println("❌ Could not connect to " + host + ":" + port + ". Is the server running?");
            System.out.println("   (You can still type commands, it will try to reconnect)");
        }

        System.out.println("\n--- Sider Shell ---");
        System.out.println("Commands: PUT <k> <v> | GET <k> | DEL <k> | COMPACT | EXIT");
        System.out.println("-------------------");

        Scanner scanner = new Scanner(System.in);

        while (true) {
            try {
                String status = client.isConnected() ? "🟢" : "🔴";
                System.out.print(status + " sider> ");
                
                if (!scanner.hasNextLine()) break;
                String input = scanner.nextLine().trim();
                
                if (input.isEmpty()) continue;

                String[] parts = input.split("\\s+");
                String cmd = parts[0].toUpperCase();

                if (cmd.equals("EXIT") || cmd.equals("QUIT")) {
                    System.out.println("Bye!");
                    break;
                } 
                else if (cmd.equals("CONNECT")) {
                    if (parts.length < 2) {
                        System.out.println("Usage: CONNECT <host> [port]");
                        continue;
                    }
                    String newHost = parts[1];
                    int newPort = (parts.length > 2) ? Integer.parseInt(parts[2]) : 4000;
                    
                    client.close();
                    client = new SiderClient(newHost, newPort);
                    
                    if (client.isConnected()) {
                        System.out.println("✅ Switched to " + newHost + ":" + newPort);
                    } else {
                        System.out.println("❌ Could not reach " + newHost + ":" + newPort);
                    }
                } 
                else if (cmd.equals("PUT") || cmd.equals("GET") || cmd.equals("DEL") || cmd.equals("COMPACT")) {
                    // Send raw command
                    String response = client.sendRaw(input);
                    if (response.startsWith("Error")) {
                        System.out.println("⚠️  " + response);
                    } else {
                        System.out.println(response);
                    }
                } 
                else if (cmd.equals("HELP")) {
                    System.out.println("  PUT <key> <value>  : Save data");
                    System.out.println("  GET <key>          : Read data");
                    System.out.println("  DEL <key>          : Delete data");
                    System.out.println("  COMPACT            : Trigger disk compaction");
                    System.out.println("  CONNECT <host>     : Switch server");
                    System.out.println("  EXIT               : Quit");
                } 
                else {
                    System.out.println("Unknown command: " + cmd);
                }

            } catch (Exception e) {
                System.out.println("Error processing command: " + e.getMessage());
            }
        }
        
        client.close();
        scanner.close();
    }
}