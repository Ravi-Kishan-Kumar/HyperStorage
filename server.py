# server.py - Central coordination server
from flask import Flask, request, jsonify, render_template, send_file
from flask_cors import CORS
import sqlite3
import hashlib
import secrets
import requests
import os
import tempfile  # <--- Added for cross-platform temp directory support
from datetime import datetime, timedelta
import json
import threading
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
import numpy as np

app = Flask(__name__)
CORS(app)
app.secret_key = secrets.token_hex(32)

DB_FILE = 'hyperstorage.db'
NODE_TIMEOUT_SECONDS = 45


class FileClassifier:
    def __init__(self):
        # Expanded Training data: (filename_pattern, extension, category)
        training_data = [
            # --- DOCUMENTS (General, Legal, Academic) ---
            ('project_report.pdf', 'pdf', 'DOCUMENT'), ('thesis_final.docx', 'docx', 'DOCUMENT'),
            ('research_paper.doc', 'doc', 'DOCUMENT'), ('meeting_notes.txt', 'txt', 'DOCUMENT'),
            ('readme.md', 'md', 'DOCUMENT'), ('contract_agreement.pdf', 'pdf', 'DOCUMENT'),
            ('legal_brief.pdf', 'pdf', 'DOCUMENT'), ('proposal_draft.docx', 'docx', 'DOCUMENT'),
            ('user_manual.pdf', 'pdf', 'DOCUMENT'), ('whitepaper.pdf', 'pdf', 'DOCUMENT'),
            ('ebook_guide.epub', 'epub', 'DOCUMENT'), ('assignment.docx', 'docx', 'DOCUMENT'),
            ('letter_of_intent.pdf', 'pdf', 'DOCUMENT'), ('memo.txt', 'txt', 'DOCUMENT'),
            
            # --- FINANCIAL (Invoices, Receipts, Money) ---
            ('invoice_2024.pdf', 'pdf', 'INVOICE'), ('receipt_hotel.pdf', 'pdf', 'INVOICE'),
            ('bill_utility.pdf', 'pdf', 'INVOICE'), ('purchase_order.pdf', 'pdf', 'INVOICE'),
            ('transaction_summary.pdf', 'pdf', 'INVOICE'), ('payment_confirmation.pdf', 'pdf', 'INVOICE'),
            ('tax_return.pdf', 'pdf', 'INVOICE'), ('quotation.pdf', 'pdf', 'INVOICE'),
            ('pricing_estimate.xlsx', 'xlsx', 'INVOICE'), ('bank_statement.pdf', 'pdf', 'INVOICE'),
            
            # --- CAREER (Resumes, CVs, Portfolios) ---
            ('resume_john.pdf', 'pdf', 'RESUME'), ('cv_engineer.docx', 'docx', 'RESUME'),
            ('curriculum_vitae.pdf', 'pdf', 'RESUME'), ('cover_letter.docx', 'docx', 'RESUME'),
            ('portfolio_design.pdf', 'pdf', 'RESUME'), ('bio_data.pdf', 'pdf', 'RESUME'),
            ('application_form.pdf', 'pdf', 'RESUME'), ('references.txt', 'txt', 'RESUME'),

            # --- SPREADSHEETS (Data, Budgets, Lists) ---
            ('quarterly_data.xlsx', 'xlsx', 'SPREADSHEET'), ('annual_budget.csv', 'csv', 'SPREADSHEET'),
            ('sales_sheet.xls', 'xls', 'SPREADSHEET'), ('statistics.ods', 'ods', 'SPREADSHEET'),
            ('inventory_list.csv', 'csv', 'SPREADSHEET'), ('export_data.csv', 'csv', 'SPREADSHEET'),
            ('financial_model.xlsx', 'xlsx', 'SPREADSHEET'), ('schedule_planner.xlsx', 'xlsx', 'SPREADSHEET'),

            # --- IMAGES (Photos, Designs, Assets) ---
            ('vacation_photo.jpg', 'jpg', 'IMAGE'), ('profile_picture.png', 'png', 'IMAGE'),
            ('logo_transparent.png', 'png', 'IMAGE'), ('banner_design.jpg', 'jpg', 'IMAGE'),
            ('icon_set.gif', 'gif', 'IMAGE'), ('camera_snap.jpeg', 'jpeg', 'IMAGE'),
            ('vector_art.svg', 'svg', 'IMAGE'), ('render_3d.png', 'png', 'IMAGE'),
            ('mockup.psd', 'psd', 'IMAGE'), ('raw_capture.tiff', 'tiff', 'IMAGE'),

            # --- SCREENSHOTS (Specific type of image) ---
            ('screenshot_error.png', 'png', 'SCREENSHOT'), ('screen_capture.jpg', 'jpg', 'SCREENSHOT'),
            ('print_screen.png', 'png', 'SCREENSHOT'), ('snap_chat.png', 'png', 'SCREENSHOT'),
            ('snippet.png', 'png', 'SCREENSHOT'), ('debug_screen.jpg', 'jpg', 'SCREENSHOT'),

            # --- DIAGRAMS (Technical visuals) ---
            ('architecture_diagram.svg', 'svg', 'DIAGRAM'), ('flowchart.png', 'png', 'DIAGRAM'),
            ('uml_class.jpg', 'jpg', 'DIAGRAM'), ('network_topology.vsdx', 'vsdx', 'DIAGRAM'),
            ('blueprint.pdf', 'pdf', 'DIAGRAM'), ('schema.drawio', 'drawio', 'DIAGRAM'),

            # --- VIDEOS ---
            ('vacation_video.mp4', 'mp4', 'VIDEO'), ('movie_clip.avi', 'avi', 'VIDEO'),
            ('screen_recording.mov', 'mov', 'VIDEO'), ('render_output.mkv', 'mkv', 'VIDEO'),
            ('tutorial.mp4', 'mp4', 'VIDEO'), ('zoom_recording.mp4', 'mp4', 'VIDEO'),
            ('animation.webm', 'webm', 'VIDEO'), ('trailer.flv', 'flv', 'VIDEO'),

            # --- AUDIO ---
            ('song_track.mp3', 'mp3', 'AUDIO'), ('voice_memo.wav', 'wav', 'AUDIO'),
            ('podcast_episode.m4a', 'm4a', 'AUDIO'), ('interview_rec.ogg', 'ogg', 'AUDIO'),
            ('music_mix.flac', 'flac', 'AUDIO'), ('sound_effect.mp3', 'mp3', 'AUDIO'),

            # --- CODE (Scripts, Configs, Web) ---
            ('script_main.py', 'py', 'CODE'), ('app_logic.js', 'js', 'CODE'),
            ('index_page.html', 'html', 'CODE'), ('styles.css', 'css', 'CODE'),
            ('config_settings.json', 'json', 'CODE'), ('dockerfile', '', 'CODE'),
            ('requirements.txt', 'txt', 'CODE'), ('database_schema.sql', 'sql', 'CODE'),
            ('backend.go', 'go', 'CODE'), ('component.jsx', 'jsx', 'CODE'),
            ('interface.ts', 'ts', 'CODE'), ('makefile', '', 'CODE'),

            # --- ARCHIVES (Backups, Compressed) ---
            ('backup_full.zip', 'zip', 'ARCHIVE'), ('project_files.tar', 'tar', 'ARCHIVE'),
            ('db_dump.gz', 'gz', 'ARCHIVE'), ('compressed_folder.rar', 'rar', 'ARCHIVE'),
            ('installer.7z', '7z', 'ARCHIVE'), ('assets.zip', 'zip', 'ARCHIVE'),
        ]

        # IMPROVEMENT: Replace punctuation with spaces so "my_file.pdf" becomes "my file pdf"
        self.texts = []
        for name, ext, _ in training_data:
            clean_name = name.replace('_', ' ').replace('-', ' ').replace('.', ' ')
            self.texts.append(f"{clean_name} {ext}")
            
        self.labels = [cat for _, _, cat in training_data]

        # Train TF-IDF vectorizer and classifier
        self.vectorizer = TfidfVectorizer(max_features=200, stop_words='english')
        X = self.vectorizer.fit_transform(self.texts)

        self.classifier = MultinomialNB(alpha=0.1) # Lower alpha helps with smaller datasets
        self.classifier.fit(X, self.labels)

    def classify(self, filename, mime_type=''):
        """Classify a file and return predicted tags"""
        # Extract extension
        ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        
        # Clean filename for prediction (same processing as training)
        clean_name = filename.replace('_', ' ').replace('-', ' ').replace('.', ' ')
        text = f"{clean_name} {ext}"
        
        X = self.vectorizer.transform([text])

        # Get prediction with probability
        prediction = self.classifier.predict(X)[0]
        probabilities = self.classifier.predict_proba(X)[0]
        confidence = max(probabilities)

        tags = [prediction]
        
        # Fallback Logic:
        # If confidence is low, strictly trust the MIME type or extension
        if confidence < 0.55: 
            if mime_type.startswith('image/'):
                tags = ['IMAGE']
            elif mime_type.startswith('video/'):
                tags = ['VIDEO']
            elif mime_type.startswith('audio/'):
                tags = ['AUDIO']
            elif 'pdf' in mime_type or 'text' in mime_type:
                # Default to DOCUMENT if unsure about PDF content
                tags = ['DOCUMENT']

        return list(set(tags))

# Initialize classifier
file_classifier = FileClassifier()

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Users table (3NF compliant)
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        hashed_password TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        registration_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # Files table (3NF compliant)
    c.execute('''CREATE TABLE IF NOT EXISTS files (
        file_id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_filename TEXT NOT NULL,
        file_type TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        ai_tags TEXT,
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )''')
    
    # File_Versions table (3NF compliant)
    c.execute('''CREATE TABLE IF NOT EXISTS file_versions (
        file_id INTEGER NOT NULL,
        version_number INTEGER NOT NULL,
        file_size_MB REAL NOT NULL,
        upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        file_hash TEXT NOT NULL,
        current_status TEXT DEFAULT 'active',
        encryption_key TEXT NOT NULL,
        PRIMARY KEY (file_id, version_number),
        FOREIGN KEY (file_id) REFERENCES files(file_id)
    )''')
    
    # Nodes table (3NF compliant)
    c.execute('''CREATE TABLE IF NOT EXISTS nodes (
        node_id INTEGER PRIMARY KEY AUTOINCREMENT,
        node_name TEXT UNIQUE NOT NULL,
        node_url TEXT UNIQUE NOT NULL,
        total_capacity_GB REAL DEFAULT 100,
        used_capacity_GB REAL DEFAULT 0,
        node_status TEXT DEFAULT 'active',
        last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # File_Node_Mapping table (3NF compliant)
    c.execute('''CREATE TABLE IF NOT EXISTS file_node_mapping (
        mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER NOT NULL,
        node_id INTEGER NOT NULL,
        storage_path TEXT NOT NULL,
        shard_index INTEGER NOT NULL,
        replication_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (file_id) REFERENCES files(file_id),
        FOREIGN KEY (node_id) REFERENCES nodes(node_id)
    )''')
    
    conn.commit()
    conn.close()

init_db()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    email = data.get('email')
    password = data.get('password')
    
    if not username or not email or not password:
        return jsonify({'success': False, 'error': 'Missing fields'})
    
    hashed_password = hashlib.sha256(password.encode()).hexdigest()
    
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('INSERT INTO users (username, hashed_password, email) VALUES (?, ?, ?)', 
                  (username, hashed_password, email))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'Username or email already exists'})

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    
    hashed_password = hashlib.sha256(password.encode()).hexdigest()
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT user_id, username, email FROM users WHERE email = ? AND hashed_password = ?', 
              (email, hashed_password))
    user = c.fetchone()
    conn.close()
    
    if user:
        return jsonify({'success': True, 'user': {'id': user[0], 'username': user[1], 'email': user[2]}})
    return jsonify({'success': False})

@app.route('/api/nodes', methods=['GET'])
def get_nodes():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT node_id, node_name, node_url, node_status, used_capacity_GB, total_capacity_GB FROM nodes')
    nodes = [{'id': row[0], 'name': row[1], 'node_url': row[2], 'status': row[3], 
              'used': row[4], 'total': row[5]} for row in c.fetchall()]
    conn.close()
    return jsonify({'nodes': nodes})

@app.route('/api/node/register', methods=['POST'])
def register_node():
    data = request.json
    node_url = data.get('node_url')
    node_name = data.get('node_name', f'node_{node_url.split(":")[-1]}')

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute('INSERT INTO nodes (node_name, node_url) VALUES (?, ?)', (node_name, node_url))
        conn.commit()
        node_id = c.lastrowid
        conn.close()
        return jsonify({'success': True, 'node_id': node_id})
    except sqlite3.IntegrityError:
        c.execute('UPDATE nodes SET last_heartbeat = CURRENT_TIMESTAMP, node_status = "active" WHERE node_url = ?', (node_url,))
        c.execute('SELECT node_id FROM nodes WHERE node_url = ?', (node_url,))
        node_id = c.fetchone()[0]
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'node_id': node_id})

@app.route('/api/node/heartbeat', methods=['POST'])
def node_heartbeat():
    data = request.json
    node_url = data.get('node_url')
    node_name = data.get('node_name')

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('UPDATE nodes SET last_heartbeat = CURRENT_TIMESTAMP, node_status = "active" WHERE node_url = ?', (node_url,))
    conn.commit()
    conn.close()

    return jsonify({'success': True})

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file provided'})

    file = request.files['file']
    user_id = request.form.get('user_id')

    # Read file data
    file_data = file.read()
    file_size_bytes = len(file_data)
    file_size_mb = file_size_bytes / (1024 * 1024)

    # Generate encryption key
    encryption_key = secrets.token_hex(32)

    # Encrypt file data (simple XOR for demo)
    encrypted_data = bytes([b ^ int(encryption_key[:2], 16) for b in file_data])

    # Get active nodes
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT node_id, node_url FROM nodes WHERE node_status = "active"')
    nodes = c.fetchall()

    if not nodes:
        conn.close()
        return jsonify({'success': False, 'error': 'No active nodes available'})

    # Replication factor: each shard will be stored on 3 nodes (or all available if less than 3)
    REPLICATION_FACTOR = min(3, len(nodes))

    # Shard the file
    num_shards = max(3, len(nodes) // REPLICATION_FACTOR)
    shard_size = len(encrypted_data) // num_shards + 1
    shards = [encrypted_data[i:i+shard_size] for i in range(0, len(encrypted_data), shard_size)]

    # AI Classification
    ai_tags = file_classifier.classify(file.filename, file.content_type or '')
    ai_tags_str = ','.join(ai_tags)

    # Insert into Files table
    c.execute('INSERT INTO files (original_filename, file_type, user_id, ai_tags) VALUES (?, ?, ?, ?)',
              (file.filename, file.content_type or 'application/octet-stream', user_id, ai_tags_str))
    file_id = c.lastrowid

    # Insert into File_Versions table (version 1)
    file_hash = hashlib.sha256(file_data).hexdigest()
    c.execute('''INSERT INTO file_versions
                 (file_id, version_number, file_size_MB, file_hash, encryption_key, current_status)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (file_id, 1, file_size_mb, file_hash, encryption_key, 'active'))

    # Distribute shards to nodes with replication
    total_replications = 0
    for i, shard in enumerate(shards):
        # Select nodes for this shard using round-robin with offset
        selected_nodes = []
        for replica in range(REPLICATION_FACTOR):
            node_index = (i + replica) % len(nodes)
            selected_nodes.append(nodes[node_index])

        # Store shard on each selected node
        for node_id, node_url in selected_nodes:
            storage_path = f'/storage/node{node_id}/file_{file_id}_shard_{i}.bin'

            try:
                # Send shard to node
                response = requests.post(f'{node_url}/store',
                                        json={'file_id': file_id, 'shard_index': i, 'data': shard.hex()},
                                        timeout=5)

                if response.status_code == 200:
                    c.execute('''INSERT INTO file_node_mapping
                                (file_id, node_id, storage_path, shard_index)
                                VALUES (?, ?, ?, ?)''',
                             (file_id, node_id, storage_path, i))
                    total_replications += 1
            except:
                pass

    conn.commit()
    conn.close()

    return jsonify({'success': True, 'file_id': file_id, 'shard_count': len(shards),
                    'replications': total_replications, 'replication_factor': REPLICATION_FACTOR,
                    'ai_tags': ai_tags})

@app.route('/api/files/<int:user_id>', methods=['GET'])
def get_user_files(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''SELECT f.file_id, f.original_filename, fv.file_size_MB,
                        fv.upload_timestamp, fv.current_status,
                        COUNT(fnm.mapping_id) as shard_count, f.ai_tags
                 FROM files f
                 JOIN file_versions fv ON f.file_id = fv.file_id AND fv.version_number = 1
                 LEFT JOIN file_node_mapping fnm ON f.file_id = fnm.file_id
                 WHERE f.user_id = ?
                 GROUP BY f.file_id
                 ORDER BY fv.upload_timestamp DESC''', (user_id,))
    files = [{'id': row[0], 'filename': row[1], 'file_size_mb': row[2],
              'uploaded_at': row[3], 'status': row[4], 'shard_count': row[5],
              'ai_tags': row[6].split(',') if row[6] else []}
             for row in c.fetchall()]
    conn.close()
    return jsonify({'files': files})

@app.route('/api/download/<int:file_id>', methods=['GET'])
def download_file(file_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Get file metadata
    c.execute('''SELECT f.original_filename, fv.encryption_key
                 FROM files f
                 JOIN file_versions fv ON f.file_id = fv.file_id AND fv.version_number = 1
                 WHERE f.file_id = ?''', (file_id,))
    file_info = c.fetchone()

    if not file_info:
        conn.close()
        return jsonify({'success': False, 'error': 'File not found'})

    filename, encryption_key = file_info

    # Get all shards with ALL replica locations
    c.execute('''SELECT fnm.shard_index, n.node_url, n.node_status
                 FROM file_node_mapping fnm
                 JOIN nodes n ON fnm.node_id = n.node_id
                 WHERE fnm.file_id = ?
                 ORDER BY fnm.shard_index, n.node_status DESC''', (file_id,))
    shards_info = c.fetchall()
    conn.close()

    # Group by shard_index to handle replicas
    shard_replicas = {}
    for shard_index, node_url, node_status in shards_info:
        if shard_index not in shard_replicas:
            shard_replicas[shard_index] = []
        shard_replicas[shard_index].append((node_url, node_status))

    # Retrieve shards from nodes (try replicas if primary fails)
    shards = {}
    for shard_index in sorted(shard_replicas.keys()):
        replica_nodes = shard_replicas[shard_index]
        shard_retrieved = False

        # Try each replica until one succeeds (prioritize active nodes)
        for node_url, node_status in replica_nodes:
            try:
                response = requests.get(f'{node_url}/retrieve/{file_id}/{shard_index}', timeout=5)
                if response.status_code == 200:
                    shard_data = bytes.fromhex(response.json()['data'])
                    shards[shard_index] = shard_data
                    shard_retrieved = True
                    break
            except Exception as e:
                continue

        if not shard_retrieved:
            return jsonify({'success': False, 'error': f'Could not retrieve shard {shard_index} from any replica'})

    if not shards:
        return jsonify({'success': False, 'error': 'Could not retrieve file'})

    # Combine shards in order
    encrypted_data = b''.join([shards[i] for i in sorted(shards.keys())])

    # Decrypt
    decrypted_data = bytes([b ^ int(encryption_key[:2], 16) for b in encrypted_data])

    # Save temporarily and send (CROSS-PLATFORM FIX)
    try:
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, filename)
        
        with open(temp_path, 'wb') as f:
            f.write(decrypted_data)

        return send_file(temp_path, as_attachment=True, download_name=filename)
    except Exception as e:
        return jsonify({'success': False, 'error': f'Download failed: {str(e)}'})

# --- NEW DELETE ENDPOINT ---
@app.route('/api/delete', methods=['POST'])
def delete_file():
    data = request.json
    file_id = data.get('file_id')
    user_id = data.get('user_id')

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 1. Verify file exists and belongs to user
    c.execute('SELECT original_filename FROM files WHERE file_id = ? AND user_id = ?', (file_id, user_id))
    file_row = c.fetchone()
    
    if not file_row:
        conn.close()
        return jsonify({'success': False, 'error': 'File not found or access denied'})

    # 2. Find all nodes that have shards for this file
    c.execute('''SELECT fnm.shard_index, n.node_url 
                 FROM file_node_mapping fnm
                 JOIN nodes n ON fnm.node_id = n.node_id
                 WHERE fnm.file_id = ?''', (file_id,))
    
    mappings = c.fetchall()

    # 3. Request nodes to delete the physical files
    # We do this asynchronously so the user doesn't wait if a node is slow
    def delete_on_nodes(mappings_list, f_id):
        for shard_idx, node_url in mappings_list:
            try:
                requests.delete(f'{node_url}/delete/{f_id}/{shard_idx}', timeout=2)
            except:
                # If node is offline, we skip it (garbage collection handles it later in a real system)
                pass

    threading.Thread(target=delete_on_nodes, args=(mappings, file_id)).start()

    # 4. Clean up Database
    try:
        c.execute('DELETE FROM file_node_mapping WHERE file_id = ?', (file_id,))
        c.execute('DELETE FROM file_versions WHERE file_id = ?', (file_id,))
        c.execute('DELETE FROM files WHERE file_id = ?', (file_id,))
        conn.commit()
        success = True
    except Exception as e:
        print(f"Error deleting from DB: {e}")
        success = False

    conn.close()
    return jsonify({'success': success})

def monitor_node_health():
    """Background task to monitor node health and mark inactive nodes"""
    while True:
        try:
            time.sleep(15)
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()

            # Mark nodes as inactive if they haven't sent heartbeat in NODE_TIMEOUT_SECONDS
            c.execute('''UPDATE nodes
                        SET node_status = 'inactive'
                        WHERE node_status = 'active'
                        AND datetime(last_heartbeat) < datetime('now', '-' || ? || ' seconds')''',
                     (NODE_TIMEOUT_SECONDS,))

            if c.rowcount > 0:
                print(f'⚠️  Marked {c.rowcount} node(s) as inactive')

            conn.commit()
            conn.close()
        except Exception as e:
            print(f'❌ Health monitor error: {e}')

if __name__ == '__main__':
    # Start node health monitoring thread
    health_monitor = threading.Thread(target=monitor_node_health, daemon=True)
    health_monitor.start()
    print('💓 Node health monitoring started')

    app.run(host='0.0.0.0', port=5000, debug=True)