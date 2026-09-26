import socket
import sys
import argparse

# Configuration
AZURE_VM_IP = "20.197.19.241"

class SiderClient:
    """
    A Python driver for the Sider database.
    Handles raw TCP connections and protocol formatting.
    """
    def __init__(self, host='localhost', port=4000, auto_connect=True):
        self.host = host
        self.port = port
        self.sock = None
        if auto_connect:
            self.connect()

    def connect(self):
        """Establishes a raw TCP connection to Sider."""
        try:
            # Create a TCP/IP socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Set a timeout so the client doesn't hang forever if server dies
            self.sock.settimeout(5.0) 
            self.sock.connect((self.host, self.port))
            return True
        except (socket.error, socket.timeout) as e:
            self.sock = None
            return False

    def is_connected(self):
        return self.sock is not None

    def _send_command(self, command_str):
        """Encodes and sends a raw text command, returns the response."""
        if not self.sock:
            # Try to reconnect once
            if not self.connect():
                return "Error: Not connected to server."
        
        try:
            # Protocol: Command must end with newline
            msg = (command_str + "\n").encode('utf-8')
            self.sock.sendall(msg)
            
            # Read response. In a real driver, we'd buffer until we see a newline.
            # For this simple protocol, 4096 bytes is usually enough for a value.
            response = self.sock.recv(4096).decode('utf-8').strip()
            if not response:
                # Empty string usually means server closed connection
                self.sock.close()
                self.sock = None
                return "Error: Server closed connection."
                
            return response
        except socket.timeout:
            return "Error: Request timed out."
        except socket.error as e:
            self.sock.close()
            self.sock = None
            return f"Error: Connection lost ({e})"

    def put(self, key, value):
        return self._send_command(f"PUT {key} {value}")

    def put_with_ttl(self, key, ttl_seconds, value):
        return self._send_command(f"PUTEX {key} {ttl_seconds} {value}")

    def get(self, key):
        return self._send_command(f"GET {key}")

    def delete(self, key):
        return self._send_command(f"DEL {key}")

    def expire(self, key, ttl_seconds):
        return self._send_command(f"EXPIRE {key} {ttl_seconds}")

    def ttl(self, key):
        return self._send_command(f"TTL {key}")

    def clear(self, key_prefix):
        return self._send_command(f"CLEAR {key_prefix}")

    def subscribe(self, channel):
        return self._send_command(f"SUBSCRIBE {channel}")

    def unsubscribe(self, channel):
        return self._send_command(f"UNSUBSCRIBE {channel}")

    def publish(self, channel, message):
        return self._send_command(f"PUBLISH {channel} {message}")
    
    def compact(self):
        return self._send_command("COMPACT")

    def close(self):
        if self.sock:
            self.sock.close()

# ==========================================
# INTERACTIVE CLI
# ==========================================

def run_cli(host, port):
    print(f"🔌 Connecting to Sider at {host}:{port}...")
    client = SiderClient(host, port)
    
    if client.is_connected():
        print(f"✅ Connected! (Host: {host})")
    else:
        print(f"❌ Could not connect to {host}:{port}. Is the server running?")
        print("   (You can still type commands, it will try to reconnect)")

    print("\n--- Sider Shell ---")
    print("Commands: PUT <k> <v> | PUTEX <k> <ttl> <v> | GET <k> | DEL <k> | EXPIRE <k> <ttl> | TTL <k> | CLEAR <prefix> | SUBSCRIBE <channel> | UNSUBSCRIBE <channel> | PUBLISH <channel> <msg> | COMPACT | EXIT")
    print("-------------------")

    while True:
        try:
            # Show prompt with connection status indicator
            status = "🟢" if client.is_connected() else "🔴"
            user_input = input(f"{status} sider> ").strip()
            
            if not user_input:
                continue

            parts = user_input.split()
            cmd = parts[0].upper()

            if cmd == "EXIT" or cmd == "QUIT":
                print("Bye!")
                break
            
            elif cmd == "CONNECT":
                # Feature to switch servers inside the CLI
                if len(parts) < 2:
                    print("Usage: CONNECT <host> [port]")
                    continue
                new_host = parts[1]
                new_port = int(parts[2]) if len(parts) > 2 else 4000
                client.close()
                client = SiderClient(new_host, new_port)
                if client.is_connected():
                    print(f"✅ Switched to {new_host}:{new_port}")
                else:
                    print(f"❌ Could not reach {new_host}:{new_port}")

            elif cmd in ["PUT", "PUTEX", "GET", "DEL", "EXPIRE", "TTL", "CLEAR", "SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH", "COMPACT"]:
                # Pass raw command string directly to the helper
                # This handles the logic for us
                resp = client._send_command(user_input)
                
                # Pretty print errors
                if resp.startswith("Error"):
                    print(f"⚠️  {resp}")
                else:
                    print(resp)
            
            elif cmd == "HELP":
                print("  PUT <key> <value>  : Save data")
                print("  PUTEX <key> <ttl> <value> : Save data with expiration")
                print("  GET <key>          : Read data")
                print("  DEL <key>          : Delete data")
                print("  EXPIRE <key> <ttl>: Set expiration")
                print("  TTL <key>         : Read remaining seconds")
                print("  CLEAR <prefix>    : Delete keys by prefix")
                print("  SUBSCRIBE <channel> / UNSUBSCRIBE <channel>")
                print("  PUBLISH <channel> <message>")
                print("  COMPACT            : Trigger disk compaction")
                print("  CONNECT <host>     : Switch server")
                print("  EXIT               : Quit")

            else:
                print(f"Unknown command: {cmd}")

        except KeyboardInterrupt:
            print("\nType EXIT to quit.")
        except Exception as e:
            print(f"Error processing command: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sider Database Client")
    # Default to the Azure VM IP
    parser.add_argument("--host", default=AZURE_VM_IP, help="Server hostname or IP")
    parser.add_argument("--port", type=int, default=4000, help="Server port")
    
    args = parser.parse_args()
    run_cli(args.host, args.port)
