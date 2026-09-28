import socket
import sys
import argparse

# Configuration
AZURE_VM_IP = "20.197.19.241"
TAILSCALE_HOST = "100.95.206.7"

class SiderClient:
    """
    A Python driver for the Sider database.
    Handles raw TCP connections and protocol formatting.
    """
    def __init__(self, host='localhost', port=4000, token=None, auto_connect=True):
        self.host = host
        self.port = port
        self.token = token
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
            if self.token:
                auth_resp = self._send_command(f"AUTH {self.token}")
                if "ERR" in auth_resp:
                    print(f"⚠️  Authentication failed: {auth_resp}")
            return True
        except (socket.error, socket.timeout) as e:
            self.sock = None
            return False

    def is_connected(self):
        return self.sock is not None

    def auth(self, token):
        """Authenticates with the Sider instance."""
        self.token = token
        return self._send_command(f"AUTH {token}")

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

    def clear(self, prefix):
        return self._send_command(f"CLEAR {prefix}")

    def subscribe(self, channel):
        return self._send_command(f"SUBSCRIBE {channel}")

    def unsubscribe(self, channel):
        return self._send_command(f"UNSUBSCRIBE {channel}")

    def publish(self, channel, message):
        return self._send_command(f"PUBLISH {channel} {message}")

    def compact(self):
        return self._send_command("COMPACT")

    def ping(self):
        return self._send_command("PING")

    def info(self):
        return self._send_command("INFO")

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

def run_cli(host, port, token=None):
    """Interactive CLI loop mimicking redis-cli or sider-cli."""
    client = SiderClient(host, port, token)
    
    print("========================================")
    print("   ⚡ SIDER PYTHON INTERACTIVE CLIENT")
    print(f"   Connecting to {host}:{port}")
    if token:
        print("   Authentication: Token Enabled")
    print("========================================")

    if not client.is_connected():
        print(f"❌ Failed to connect to Sider at {host}:{port}")
        print("   Make sure the server is running and accessible.")
        print("   Type 'CONNECT <host> <port>' to retry with different settings.")
    else:
        print(f"✅ Connected to {host}:{port}")
        print("Type commands (e.g. PUT mykey myval, GET mykey). Type 'HELP' or 'EXIT'.\n")

    while True:
        try:
            # Simple prompt
            user_input = input(f"{host}:{port}> ").strip()
            
            if not user_input:
                continue

            parts = user_input.split()
            cmd = parts[0].upper()

            if cmd == "EXIT":
                print("Goodbye!")
                client.close()
                break

            elif cmd == "CONNECT":
                if len(parts) < 2:
                    print("Usage: CONNECT <host> [port] [token]")
                    continue
                new_host = parts[1]
                new_port = int(parts[2]) if len(parts) > 2 else 4000
                new_token = parts[3] if len(parts) > 3 else None
                client.close()
                client = SiderClient(new_host, new_port, new_token)
                if client.is_connected():
                    print(f"✅ Switched to {new_host}:{new_port}")
                else:
                    print(f"❌ Could not reach {new_host}:{new_port}")

            elif cmd in ["AUTH", "PING", "INFO", "PUT", "PUTEX", "GET", "DEL", "EXPIRE", "TTL", "CLEAR", "SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH", "COMPACT"]:
                resp = client._send_command(user_input)
                if resp.startswith("Error"):
                    print(f"⚠️  {resp}")
                else:
                    print(resp)
            
            elif cmd == "HELP":
                print("  AUTH <token>       : Authenticate with database")
                print("  PING               : Health check")
                print("  INFO               : Server engine telemetry")
                print("  PUT <key> <value>  : Save data")
                print("  PUTEX <key> <ttl> <val> : Save data with expiration")
                print("  GET <key>          : Read data")
                print("  DEL <key>          : Delete data")
                print("  EXPIRE <key> <ttl> : Set expiration")
                print("  TTL <key>          : Read remaining seconds")
                print("  CLEAR <prefix>     : Delete keys by prefix")
                print("  SUBSCRIBE <ch> / UNSUBSCRIBE <ch>")
                print("  PUBLISH <ch> <msg> : Broadcast message")
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
    parser.add_argument("--host", default=TAILSCALE_HOST, help="Server hostname or IP (defaults to Tailscale desktop)")
    parser.add_argument("--port", type=int, default=4100, help="Server port")
    parser.add_argument("--token", "-t", default=None, help="Database auth token")
    
    args = parser.parse_args()
    run_cli(args.host, args.port, args.token)
