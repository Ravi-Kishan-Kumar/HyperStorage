from flask import Flask, request, jsonify
import os
import sys
import requests
import threading
import time

app = Flask(__name__)

# Configuration
NODE_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5001
NODE_NAME = f'node_{NODE_PORT}'
STORAGE_DIR = f'{NODE_NAME}_storage'
SERVER_URL = 'http://localhost:5000'
HEARTBEAT_INTERVAL = 30

# Create storage directory
os.makedirs(STORAGE_DIR, exist_ok=True)

@app.route('/store', methods=['POST'])
def store_shard():
    """Store a file shard"""
    data = request.json
    file_id = data.get('file_id')
    shard_index = data.get('shard_index')
    shard_data = data.get('data')
    
    # Store shard to disk
    shard_path = os.path.join(STORAGE_DIR, f'file_{file_id}_shard_{shard_index}.bin')
    with open(shard_path, 'w') as f:
        f.write(shard_data)
    
    print(f'✅ Stored shard {shard_index} for file {file_id}')
    return jsonify({'success': True})

@app.route('/retrieve/<int:file_id>/<int:shard_index>', methods=['GET'])
def retrieve_shard(file_id, shard_index):
    """Retrieve a file shard"""
    shard_path = os.path.join(STORAGE_DIR, f'file_{file_id}_shard_{shard_index}.bin')
    
    if not os.path.exists(shard_path):
        return jsonify({'success': False, 'error': 'Shard not found'}), 404
    
    with open(shard_path, 'r') as f:
        data = f.read()
    
    print(f'📤 Retrieved shard {shard_index} for file {file_id}')
    return jsonify({'success': True, 'data': data})

# --- NEW DELETE ENDPOINT ---
@app.route('/delete/<int:file_id>/<int:shard_index>', methods=['DELETE'])
def delete_shard(file_id, shard_index):
    """Delete a specific shard"""
    shard_filename = f'file_{file_id}_shard_{shard_index}.bin'
    shard_path = os.path.join(STORAGE_DIR, shard_filename)
    
    try:
        if os.path.exists(shard_path):
            os.remove(shard_path)
            print(f'🗑️  Deleted shard {shard_index} for file {file_id}')
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Shard not found'}), 404
    except Exception as e:
        print(f'❌ Error deleting shard: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    files = os.listdir(STORAGE_DIR)
    return jsonify({
        'status': 'active', 
        'storage_dir': STORAGE_DIR,
        'stored_shards': len(files)
    })

def register_with_server():
    """Register this node with the central server"""
    try:
        node_url = f'http://localhost:{NODE_PORT}'
        response = requests.post(f'{SERVER_URL}/api/node/register',
                               json={'node_url': node_url, 'node_name': NODE_NAME},
                               timeout=5)
        if response.status_code == 200:
            print(f'✅ Node registered with server at {node_url}')
            print(f'   Name: {NODE_NAME}')
        else:
            print(f'❌ Failed to register node')
    except Exception as e:
        print(f'❌ Could not connect to server: {e}')
        print(f'⚠️  Make sure the server is running on {SERVER_URL}')

def send_heartbeat():
    """Send heartbeat to server every 30 seconds"""
    while True:
        try:
            time.sleep(HEARTBEAT_INTERVAL)
            node_url = f'http://localhost:{NODE_PORT}'
            files = os.listdir(STORAGE_DIR)
            response = requests.post(f'{SERVER_URL}/api/node/heartbeat',
                                   json={
                                       'node_url': node_url,
                                       'node_name': NODE_NAME,
                                       'stored_shards': len(files)
                                   },
                                   timeout=5)
            if response.status_code == 200:
                print(f'💓 Heartbeat sent - {len(files)} shards stored')
        except Exception as e:
            print(f'⚠️  Heartbeat failed: {e}')

if __name__ == '__main__':
    print('='*50)
    print(f'🗄️  HyperStorage Node Starting')
    print('='*50)
    print(f'Port: {NODE_PORT}')
    print(f'Storage: {STORAGE_DIR}')
    print(f'Heartbeat: Every {HEARTBEAT_INTERVAL}s')
    print('='*50)

    register_with_server()

    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()

    app.run(host='0.0.0.0', port=NODE_PORT, debug=False)