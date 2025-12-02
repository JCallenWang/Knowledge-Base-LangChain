document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const fileStatus = document.getElementById('file-status');
    const statusIndicator = document.getElementById('status-indicator');
    const dbNameDisplay = document.getElementById('db-name');
    const userInput = document.getElementById('user-input');
    const sendBtn = document.getElementById('send-btn');
    const messagesArea = document.getElementById('messages-area');
    const clearChatBtn = document.getElementById('clear-chat');

    // Management Elements
    const navBtns = document.querySelectorAll('.nav-btn');
    const views = document.querySelectorAll('.main-content');
    const agentInstructions = document.getElementById('agent-instructions');
    const saveConfigBtn = document.getElementById('save-config-btn');
    const fileList = document.getElementById('file-list');

    let sessionId = null;

    // Tab Navigation
    navBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const tabName = btn.dataset.tab;

            // Update Buttons
            navBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // Update Views
            views.forEach(v => v.classList.remove('active'));
            document.getElementById(`view-${tabName}`).classList.add('active');

            // Load data if switching to management
            if (tabName === 'management') {
                loadInstructions();
                loadFiles();
            }
        });
    });

    // Config Management
    async function loadInstructions() {
        try {
            const response = await fetch('/config/instructions');
            const data = await response.json();
            agentInstructions.value = data.instructions;
        } catch (error) {
            console.error('Failed to load instructions:', error);
        }
    }

    saveConfigBtn.addEventListener('click', async () => {
        const instructions = agentInstructions.value;
        try {
            const response = await fetch('/config/instructions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ instructions })
            });

            if (response.ok) {
                alert('Instructions saved successfully!');
            } else {
                throw new Error('Failed to save');
            }
        } catch (error) {
            alert('Error saving instructions: ' + error.message);
        }
    });

    // File Management
    async function loadFiles() {
        try {
            const response = await fetch('/files');
            const data = await response.json();
            renderFileList(data.files);
        } catch (error) {
            console.error('Failed to load files:', error);
        }
    }

    function renderFileList(files) {
        fileList.innerHTML = '';
        if (files.length === 0) {
            fileList.innerHTML = '<p style="color: var(--text-secondary);">No processed datasets found.</p>';
            return;
        }

        files.forEach(file => {
            const card = document.createElement('div');
            card.className = 'file-card';

            // Extract clean name (remove hash suffix if possible)
            // Format: name_hash
            let displayName = file;
            if (file.includes('_')) {
                const parts = file.split('_');
                if (parts.length > 1 && parts[parts.length - 1].length === 8) {
                    displayName = parts.slice(0, -1).join('_');
                }
            }

            card.innerHTML = `
                <div class="file-info">
                    <span class="file-icon">📊</span>
                    <span class="file-name" title="${file}">${displayName}</span>
                </div>
                <button class="btn-delete" title="Delete Dataset" data-folder="${file}">
                    🗑️
                </button>
            `;
            fileList.appendChild(card);
        });

        // Add delete listeners
        document.querySelectorAll('.btn-delete').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const folder = e.currentTarget.dataset.folder;
                if (confirm(`Are you sure you want to delete "${folder}"? This cannot be undone.`)) {
                    deleteFile(folder);
                }
            });
        });
    }

    async function deleteFile(folderName) {
        try {
            const response = await fetch(`/files/${folderName}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                loadFiles(); // Refresh list
            } else {
                const error = await response.json();
                alert('Failed to delete: ' + error.detail);
            }
        } catch (error) {
            alert('Error deleting file: ' + error.message);
        }
    }

    // Drag & Drop Handlers
    dropZone.addEventListener('click', () => fileInput.click());

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('dragover');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');
        const files = e.dataTransfer.files;
        if (files.length > 0) {
            handleFileUpload(files[0]);
        }
    });

    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length > 0) {
            handleFileUpload(e.target.files[0]);
        }
    });

    async function handleFileUpload(file) {
        if (!file.name.endsWith('.xlsx') && !file.name.endsWith('.xls')) {
            showStatus('Invalid file format. Please upload Excel file.', 'error');
            return;
        }

        showStatus('Uploading and processing...', 'info');
        statusIndicator.textContent = 'Processing...';
        statusIndicator.style.color = '#fbbf24'; // Warning color

        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Upload failed');
            }

            const data = await response.json();
            sessionId = data.session_id;

            showStatus(`Loaded: ${file.name}`, 'success');
            statusIndicator.textContent = 'Active';
            statusIndicator.style.color = '#10b981'; // Success color
            dbNameDisplay.textContent = file.name.replace(/\.[^/.]+$/, "");

            enableChat();
            addSystemMessage(`File '${file.name}' processed successfully. You can now ask questions.`);

        } catch (error) {
            showStatus(`Error: ${error.message}`, 'error');
            statusIndicator.textContent = 'Error';
            statusIndicator.style.color = '#ef4444';
        }
    }

    function showStatus(message, type) {
        fileStatus.textContent = message;
        fileStatus.className = 'file-status ' + type;
    }

    function enableChat() {
        userInput.disabled = false;
        sendBtn.disabled = false;
        userInput.focus();
    }

    // Chat Logic
    sendBtn.addEventListener('click', sendMessage);
    userInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });

    clearChatBtn.addEventListener('click', () => {
        messagesArea.innerHTML = '';
        addSystemMessage('Chat cleared.');
    });

    async function sendMessage() {
        const text = userInput.value.trim();
        if (!text || !sessionId) return;

        // Add User Message
        addMessage(text, 'user');
        userInput.value = '';
        userInput.disabled = true;
        sendBtn.disabled = true;

        // Add Loading Indicator
        const loadingId = addLoadingIndicator();

        try {
            const response = await fetch('/chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    session_id: sessionId,
                    message: text
                })
            });

            removeMessage(loadingId);

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Chat failed');
            }

            const data = await response.json();

            // Add Agent Response
            addMessage(data.response, 'agent', data.sql_query);

        } catch (error) {
            removeMessage(loadingId);
            addMessage(`Error: ${error.message}`, 'agent');
        } finally {
            userInput.disabled = false;
            sendBtn.disabled = false;
            userInput.focus();
        }
    }

    function addMessage(text, type, sql = null) {
        const msgDiv = document.createElement('div');
        msgDiv.className = `message ${type}`;

        let contentHtml = `<div class="content">${text}</div>`;

        if (sql) {
            contentHtml += `
                <div class="meta">
                    <span>Generated SQL:</span>
                    <div class="sql-block">${sql}</div>
                </div>
            `;
        }

        msgDiv.innerHTML = contentHtml;
        messagesArea.appendChild(msgDiv);
        scrollToBottom();
    }

    function addSystemMessage(text) {
        const msgDiv = document.createElement('div');
        msgDiv.className = 'message system';
        msgDiv.innerHTML = `<div class="content">${text}</div>`;
        messagesArea.appendChild(msgDiv);
        scrollToBottom();
    }

    function addLoadingIndicator() {
        const id = 'loading-' + Date.now();
        const msgDiv = document.createElement('div');
        msgDiv.id = id;
        msgDiv.className = 'message agent';
        msgDiv.innerHTML = `
            <div class="content">
                <div class="typing-indicator">
                    <div class="dot"></div>
                    <div class="dot"></div>
                    <div class="dot"></div>
                </div>
            </div>
        `;
        messagesArea.appendChild(msgDiv);
        scrollToBottom();
        return id;
    }

    function removeMessage(id) {
        const el = document.getElementById(id);
        if (el) el.remove();
    }

    function scrollToBottom() {
        messagesArea.scrollTop = messagesArea.scrollHeight;
    }
});
