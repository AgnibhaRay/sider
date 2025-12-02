import socket

class SiderClient:
    def __init__(self, host='20.197.19.241', port=4000):
        self.host = host
        self.port = port
        self.sock = None
        self.connect()

    def connect(self):
        """Establishes a raw TCP connection to Sider."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.host, self.port))
            print(f"✅ Connected to Sider at {self.host}:{self.port}")
        except ConnectionRefusedError:
            print(f"❌ Could not connect. Is Sider running on port {self.port}?")
            self.sock = None

    def _send_command(self, command_str):
        """Encodes and sends a raw text command, returns the response."""
        if not self.sock:
            return "Error: Not connected"
        
        try:
            # Send command with newline
            self.sock.sendall((command_str + "\n").encode('utf-8'))
            
            # Read response (simple buffer read for demo)
            response = self.sock.recv(4096).decode('utf-8').strip()
            return response
        except BrokenPipeError:
            print("❌ Connection lost. Reconnecting...")
            self.connect()
            return self._send_command(command_str)

    def put(self, key, value):
        return self._send_command(f"PUT {key} {value}")

    def get(self, key):
        return self._send_command(f"GET {key}")

    def delete(self, key):
        return self._send_command(f"DEL {key}")
    
    def compact(self):
        return self._send_command("COMPACT")

    def close(self):
        if self.sock:
            self.sock.close()

# ==========================================
# DEMO USAGE
# ==========================================
if __name__ == "__main__":
    import time
    
    # 1. Initialize Client
    client = SiderClient()

    if client.sock:
        # 2. Write Data
        print(">> Writing Data...")
        print(f"PUT user:100 -> {client.put('user:100', 'Alice')}")
        print(f"PUT user:101 -> {client.put('user:101', 'Bob')}")
        print(f"PUT config:mode -> {client.put('config:mode', 'production')}")

        # 3. Read Data
        print("\n>> Reading Data...")
        print(f"GET user:100: {client.get('user:100')}")
        print(f"GET user:101: {client.get('user:101')}")
        print(f"GET unknown:  {client.get('unknown_key')}")

        # 4. Persistence Test
        # (You can kill the Go server here and restart it to test this manually)
        
        # 5. Delete Data
        print("\n>> Deleting user:100...")
        client.delete('user:100')
        print(f"GET user:100: {client.get('user:100')}")
        
        client.close()