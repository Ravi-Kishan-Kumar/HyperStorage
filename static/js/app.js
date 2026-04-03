let currentUser = null;
let allFiles = [];
let currentTagFilter = 'ALL';

function showRegister() {
    document.getElementById('loginForm').classList.add('hidden');
    document.getElementById('registerForm').classList.remove('hidden');
}

function showLogin() {
    document.getElementById('registerForm').classList.add('hidden');
    document.getElementById('loginForm').classList.remove('hidden');
}

async function register() {
    const username = document.getElementById('regUsername').value;
    const email = document.getElementById('regEmail').value;
    const password = document.getElementById('regPassword').value;

    const res = await fetch('/api/register', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({username, email, password})
    });

    const data = await res.json();
    if (data.success) {
        alert('Registration successful! Please login.');
        showLogin();
    } else {
        alert('Error: ' + data.error);
    }
}

async function login() {
    const email = document.getElementById('loginEmail').value;
    const password = document.getElementById('loginPassword').value;

    const res = await fetch('/api/login', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({email, password})
    });

    const data = await res.json();
    if (data.success) {
        currentUser = data.user;
        document.getElementById('authView').classList.add('hidden');
        document.getElementById('dashboardView').classList.remove('hidden');
        document.getElementById('userEmail').textContent = currentUser.email;
        loadNodes();
        loadFiles();
    } else {
        alert('Invalid credentials');
    }
}

function logout() {
    currentUser = null;
    document.getElementById('dashboardView').classList.add('hidden');
    document.getElementById('authView').classList.remove('hidden');
}

async function loadNodes() {
    const res = await fetch('/api/nodes');
    const data = await res.json();

    const nodeList = document.getElementById('nodeList');
    nodeList.innerHTML = data.nodes.map(node => `
        <div class="node ${node.status}">
            <strong>${node.name.toUpperCase()}</strong><br>
            <small>${node.node_url.split('//')[1]}</small><br>
            <span class="status ${node.status === 'active' ? 'success' : 'error'}">${node.status}</span>
        </div>
    `).join('');
}

async function uploadFile() {
    const fileInput = document.getElementById('fileInput');
    const file = fileInput.files[0];

    if (!file) {
        alert('Please select a file');
        return;
    }

    const statusDiv = document.getElementById('uploadStatus');
    statusDiv.innerHTML = '<span class="status">UPLOADING...</span>';

    const formData = new FormData();
    formData.append('file', file);
    formData.append('user_id', currentUser.id);

    const res = await fetch('/api/upload', {
        method: 'POST',
        body: formData
    });

    const data = await res.json();
    if (data.success) {
        const tags = data.ai_tags ? data.ai_tags.map(t => `<span class="ai-tag ${t}">${t}</span>`).join('') : '';
        statusDiv.innerHTML = '<span class="status success">UPLOADED - ' + data.shard_count + ' SHARDS DISTRIBUTED</span><br>' +
                             '<span style="margin-top: 8px; display: inline-block;"><span class="ai-badge">AI CLASSIFIED:</span> ' + tags + '</span>';
        fileInput.value = '';
        setTimeout(() => loadFiles(), 500);
    } else {
        statusDiv.innerHTML = '<span class="status error">UPLOAD FAILED: ' + data.error + '</span>';
    }
}

async function loadFiles() {
    const res = await fetch('/api/files/' + currentUser.id);
    const data = await res.json();

    allFiles = data.files;

    // Build tag filter buttons
    buildTagFilters();

    // Display files
    displayFiles(allFiles);
}

function buildTagFilters() {
    // Collect all unique tags
    const allTags = new Set();
    allFiles.forEach(f => {
        if (f.ai_tags && f.ai_tags.length > 0) {
            f.ai_tags.forEach(tag => allTags.add(tag));
        }
    });

    const tagFilters = document.getElementById('tagFilters');
    const sortedTags = Array.from(allTags).sort();

    tagFilters.innerHTML = `
        <button class="filter-btn ${currentTagFilter === 'ALL' ? 'active' : ''}" data-tag="ALL" onclick="filterByTag('ALL')">ALL</button>
        ${sortedTags.map(tag => `
            <button class="filter-btn ${currentTagFilter === tag ? 'active' : ''}" data-tag="${tag}" onclick="filterByTag('${tag}')">
                <span class="ai-tag ${tag}" style="margin: 0; padding: 4px 8px;">${tag}</span>
            </button>
        `).join('')}
    `;
}

function displayFiles(files) {
    const fileList = document.getElementById('fileList');

    if (files.length === 0) {
        fileList.innerHTML = '<p>No files found.</p>';
        return;
    }

    fileList.innerHTML = files.map(f => `
        <div class="file-item">
            <div>
                <strong>${f.filename}</strong>
                <span class="ai-badge">AI</span><br>
                ${f.ai_tags && f.ai_tags.length > 0 ? f.ai_tags.map(tag => `<span class="ai-tag ${tag}">${tag}</span>`).join('') : ''}
                <br>
                <small>${f.file_size_mb.toFixed(2)} MB · ${f.shard_count} SHARDS · ${new Date(f.uploaded_at).toLocaleString()}</small>
            </div>
            <div class="actions">
                <button onclick="downloadFile(${f.id}, '${f.filename}')">DOWNLOAD</button>
                <button class="btn-delete" onclick="deleteFile(${f.id})">DELETE</button>
            </div>
        </div>
    `).join('');
}

function filterByTag(tag) {
    currentTagFilter = tag;

    // Update active button
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tag === tag);
    });

    filterFiles();
}

function filterFiles() {
    const searchTerm = document.getElementById('searchInput').value.toLowerCase();
    const sortBy = document.getElementById('sortSelect').value;

    let filtered = allFiles.filter(f => {
        // Filter by tag
        const tagMatch = currentTagFilter === 'ALL' ||
                        (f.ai_tags && f.ai_tags.includes(currentTagFilter));

        // Filter by search term (filename or tags)
        const searchMatch = searchTerm === '' ||
                           f.filename.toLowerCase().includes(searchTerm) ||
                           (f.ai_tags && f.ai_tags.some(tag => tag.toLowerCase().includes(searchTerm)));

        return tagMatch && searchMatch;
    });

    // Sort files
    filtered.sort((a, b) => {
        switch(sortBy) {
            case 'date-desc':
                return new Date(b.uploaded_at) - new Date(a.uploaded_at);
            case 'date-asc':
                return new Date(a.uploaded_at) - new Date(b.uploaded_at);
            case 'name-asc':
                return a.filename.localeCompare(b.filename);
            case 'name-desc':
                return b.filename.localeCompare(a.filename);
            case 'size-desc':
                return b.file_size_mb - a.file_size_mb;
            case 'size-asc':
                return a.file_size_mb - b.file_size_mb;
            default:
                return 0;
        }
    });

    displayFiles(filtered);
}

async function downloadFile(fileId, filename) {
    window.location.href = '/api/download/' + fileId;
}

// --- NEW DELETE FUNCTION ---
async function deleteFile(fileId) {
    if (!confirm('Are you sure you want to permanently delete this file?')) {
        return;
    }

    const res = await fetch('/api/delete', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            file_id: fileId,
            user_id: currentUser.id
        })
    });

    const data = await res.json();
    if (data.success) {
        // Remove from local list and refresh UI
        allFiles = allFiles.filter(f => f.id !== fileId);
        filterFiles(); // Re-applies current filters
    } else {
        alert('Failed to delete file: ' + (data.error || 'Unknown error'));
    }
}