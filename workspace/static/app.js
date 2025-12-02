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

    let sessionId = null;

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
