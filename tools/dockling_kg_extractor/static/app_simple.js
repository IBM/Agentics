// API base URL
const API_BASE = 'http://localhost:5000/api';

console.log('Simple App.js loaded');

// State
let currentEntity = '';
let dataLoaded = false;

// State for uploaded file
let uploadedFile = null;
let uploadedFileUrl = null;

// DOM Elements
const fileInput = document.getElementById('fileInput');
const uploadArea = document.getElementById('uploadArea');
const urlInput = document.getElementById('urlInput');
const loadUrlBtn = document.getElementById('loadUrlBtn');
const sourceFile = document.getElementById('sourceFile');
const sourceUrl = document.getElementById('sourceUrl');
const fileUploadSection = document.getElementById('fileUploadSection');
const urlInputSection = document.getElementById('urlInputSection');
const folderSelect = document.getElementById('folderSelect');
const documentInfo = document.getElementById('documentInfo');
const docName = document.getElementById('docName');
const docSize = document.getElementById('docSize');
const processingOptions = document.getElementById('processingOptions');
const processBtn = document.getElementById('processBtn');
const saveLocation = document.getElementById('saveLocation');
const browseSaveBtn = document.getElementById('browseSaveBtn');
const entityInput = document.getElementById('entityInput');
const numDocsSlider = document.getElementById('numDocsSlider');
const numDocsValue = document.getElementById('numDocsValue');
const extractBtn = document.getElementById('extractBtn');
const infoboxResults = document.getElementById('infoboxResults');
const extractInfo = document.getElementById('extractInfo');
const loadingOverlay = document.getElementById('loadingOverlay');
const loadingText = document.getElementById('loadingText');
const chunkingMode = document.getElementById('chunkingMode');
const chunkSize = document.getElementById('chunkSize');
const chunkSizeValue = document.getElementById('chunkSizeValue');
const chunkSizeLabel = document.getElementById('chunkSizeLabel');
const statusText = document.getElementById('statusText');
const searchSection = document.getElementById('searchSection');
const extractSection = document.getElementById('extractSection');
const clearDataBtn = document.getElementById('clearDataBtn');

// Schema Builder Elements
const schemaName = document.getElementById('schemaName');
const schemaFieldsBody = document.getElementById('schemaFieldsBody');
const addFieldBtn = document.getElementById('addFieldBtn');
const schemaInstructions = document.getElementById('schemaInstructions');
const schemaAsList = document.getElementById('schemaAsList');
const extractSchemaBtn = document.getElementById('extractSchemaBtn');
const saveSchemaBtn = document.getElementById('saveSchemaBtn');
const clearSchemaBtn = document.getElementById('clearSchemaBtn');
const schemaResults = document.getElementById('schemaResults');

// Schema state
let schemaFields = [];
let fieldIdCounter = 0;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    setupEventListeners();
    checkStatus();
    loadAvailableFolders();
});

function setupEventListeners() {
    // Source type toggle
    sourceFile.addEventListener('change', () => {
        fileUploadSection.style.display = 'block';
        urlInputSection.style.display = 'none';
        processingOptions.style.display = 'none';
    });

    sourceUrl.addEventListener('change', () => {
        fileUploadSection.style.display = 'none';
        urlInputSection.style.display = 'block';
        processingOptions.style.display = 'none';
    });

    // Folder selection
    folderSelect.addEventListener('change', handleFolderSelect);

    // Upload area
    uploadArea.addEventListener('click', () => fileInput.click());
    fileInput.addEventListener('change', handleFileSelect);

    // URL loading
    loadUrlBtn.addEventListener('click', handleUrlLoad);
    urlInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') handleUrlLoad();
    });

    // Process button
    processBtn.addEventListener('click', handleProcessDocument);

    // Browse save location (note: browser security limits this)
    browseSaveBtn.addEventListener('click', () => {
        alert('Note: Due to browser security, you can only specify a filename. The file will be saved to your default downloads folder.');
    });

    // Drag and drop
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('drag-over');
    });

    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('drag-over');
    });

    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('drag-over');
        const files = e.dataTransfer.files;
        if (files.length > 0) {
            fileInput.files = files;
            handleFileSelect();
        }
    });


    // Num Documents slider
    if (numDocsSlider && numDocsValue) {
        numDocsSlider.addEventListener('input', (e) => {
            numDocsValue.textContent = e.target.value;
        });
    }

    chunkSize.addEventListener('input', (e) => {
        chunkSizeValue.textContent = e.target.value;
    });

    chunkingMode.addEventListener('change', (e) => {
        if (e.target.value === 'paragraphs') {
            chunkSizeLabel.textContent = 'Chunk Size (not used):';
            chunkSize.disabled = true;
            chunkSize.value = 1024;
            chunkSizeValue.textContent = 'N/A';
        } else if (e.target.value === 'fine_paragraphs') {
            chunkSizeLabel.textContent = 'Max Words per Chunk:';
            chunkSize.disabled = false;
            chunkSize.min = 50;
            chunkSize.max = 500;
            chunkSize.value = 150;
            chunkSize.step = 25;
            chunkSizeValue.textContent = '150';
        } else if (e.target.value === 'sentences') {
            chunkSizeLabel.textContent = 'Sentences per chunk:';
            chunkSize.disabled = false;
            chunkSize.min = 1;
            chunkSize.max = 10;
            chunkSize.value = 1;
            chunkSize.step = 1;
            chunkSizeValue.textContent = '1';
        } else {
            chunkSizeLabel.textContent = 'Chunk Size (words):';
            chunkSize.disabled = false;
            chunkSize.min = 100;
            chunkSize.max = 4096;
            chunkSize.value = 1024;
            chunkSize.step = 100;
            chunkSizeValue.textContent = '1024';
        }
    });

    // Initialize chunk size state on page load
    if (chunkingMode.value === 'fine_paragraphs') {
        chunkSizeLabel.textContent = 'Max Words per Chunk:';
        chunkSize.disabled = false;
        chunkSize.min = 50;
        chunkSize.max = 500;
        chunkSize.value = 150;
        chunkSize.step = 25;
        chunkSizeValue.textContent = '150';
    } else if (chunkingMode.value === 'paragraphs') {
        chunkSizeLabel.textContent = 'Chunk Size (not used):';
        chunkSize.disabled = true;
        chunkSizeValue.textContent = 'N/A';
    }

    // Entity input
    entityInput.addEventListener('input', (e) => {
        currentEntity = e.target.value.trim();
    });

    entityInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            handleExtract();
        }
    });

    // Buttons
    extractBtn.addEventListener('click', handleExtract);
    clearDataBtn.addEventListener('click', handleClearData);

    // Schema Builder
    addFieldBtn.addEventListener('click', addSchemaField);
    extractSchemaBtn.addEventListener('click', handleSchemaExtract);
    saveSchemaBtn.addEventListener('click', handleSchemaSave);
    clearSchemaBtn.addEventListener('click', handleSchemaClear);
    schemaName.addEventListener('input', updateSchemaButtons);
}

function handleClearData() {
    if (confirm('Are you sure you want to clear the current data? You will need to upload a new file.')) {
        // Reset state
        dataLoaded = false;
        currentEntity = '';

        // Reset UI
        statusText.textContent = 'No data loaded';
        statusText.className = 'status-badge status-empty';
        clearDataBtn.style.display = 'none';
        infoboxResults.innerHTML = '';
        entityInput.value = '';
        fileInput.value = '';

        // Disable tabs and switch to ingestion
        disableTabs();

        // Note: Backend state will be cleared on next upload
        alert('Data cleared. Please upload a new file with your desired chunking settings.');
    }
}

async function checkStatus() {
    try {
        const response = await fetch(`${API_BASE}/status`);
        const data = await response.json();

        if (data.data_loaded) {
            dataLoaded = true;
            statusText.textContent = `Loaded: ${data.filename} (${data.row_count} rows)`;
            statusText.className = 'status-badge status-loaded';
            clearDataBtn.style.display = 'inline-block';
            enableTabs();
        }
    } catch (error) {
        console.error('Error checking status:', error);
    }
}

function enableTabs() {
    // Enable extract and schema tabs
    document.getElementById('extract-tab').disabled = false;
    document.getElementById('schema-tab').disabled = false;
    updateSchemaButtons();
}

function disableTabs() {
    // Disable extract and schema tabs
    document.getElementById('extract-tab').disabled = true;
    document.getElementById('schema-tab').disabled = true;
    // Switch back to ingestion tab
    document.getElementById('ingestion-tab').click();
}
function handleFileSelect() {
    const file = fileInput.files[0];
    if (!file) return;

    uploadedFile = file;
    uploadedFileUrl = null;

    // Show document info
    documentInfo.style.display = 'block';
    docName.textContent = file.name;
    docSize.textContent = formatFileSize(file.size);

    // Show processing options
    processingOptions.style.display = 'block';
}

async function loadAvailableFolders() {
    try {
        const response = await fetch(`${API_BASE}/list-folders`);
        const result = await response.json();

        if (result.success && result.folders.length > 0) {
            folderSelect.innerHTML = '<option value="">-- Select a preprocessed document --</option>';
            result.folders.forEach(folder => {
                const option = document.createElement('option');
                option.value = folder.name;
                option.textContent = `${folder.name} (${folder.csv_count} CSV files)`;
                folderSelect.appendChild(option);
            });
        } else {
            folderSelect.innerHTML = '<option value="">No preprocessed documents available</option>';
        }
    } catch (error) {
        console.error('Error loading folders:', error);
        folderSelect.innerHTML = '<option value="">Error loading folders</option>';
    }
}

async function handleFolderSelect() {
    const folderName = folderSelect.value;
    if (!folderName) return;

    try {
        showLoading(`Loading preprocessed document: ${folderName}...`);

        // Load the first CSV file from the selected folder
        const response = await fetch(`${API_BASE}/list-folders`);
        const result = await response.json();

        if (result.success) {
            const folder = result.folders.find(f => f.name === folderName);
            if (folder && folder.csv_files.length > 0) {
                // Load the first CSV file
                const csvFileName = folder.csv_files[0];
                const csvPath = `${folder.path}/${csvFileName}`;

                // Upload the CSV file by path using FormData
                const formData = new FormData();
                formData.append('csv_path', csvPath);
                formData.append('is_preprocessed_csv', 'true');
                formData.append('chunk_size', '1024');
                formData.append('chunking_mode', 'words');

                const uploadResponse = await fetch(`${API_BASE}/upload`, {
                    method: 'POST',
                    body: formData
                });

                const uploadResult = await uploadResponse.json();

                if (uploadResult.success) {
                    dataLoaded = true;
                    statusText.textContent = `Loaded: ${folderName} (${uploadResult.row_count} rows)`;
                    statusText.className = 'status-badge status-loaded';
                    clearDataBtn.style.display = 'inline-block';
                    enableTabs();

                    hideLoading();
                    alert(`✓ Preprocessed document loaded successfully!\n\nFolder: ${folderName}\nRows: ${uploadResult.row_count}`);

                    // Switch to schema tab
                    const schemaTab = document.getElementById('schema-tab');
                    if (schemaTab) {
                        schemaTab.click();
                    }
                } else {
                    hideLoading();
                    alert('Error loading document: ' + (uploadResult.message || 'Unknown error'));
                }
            }
        }
    } catch (error) {
        hideLoading();
        console.error('Error loading folder:', error);
        alert('Error loading preprocessed document: ' + error.message);
    }
}

async function handleUrlLoad() {
    const url = urlInput.value.trim();
    if (!url) {
        alert('Please enter a URL');
        return;
    }

    if (!url.startsWith('http://') && !url.startsWith('https://')) {
        alert('Please enter a valid URL starting with http:// or https://');
        return;
    }

    uploadedFile = null;
    uploadedFileUrl = url;

    // Extract filename from URL
    const filename = url.split('/').pop() || 'document';

    // Show document info
    documentInfo.style.display = 'block';
    docName.textContent = filename;
    docSize.textContent = 'URL';

    // Show processing options
    processingOptions.style.display = 'block';
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

async function handleProcessDocument() {
    if (!uploadedFile && !uploadedFileUrl) {
        alert('Please select a file or enter a URL first');
        return;
    }

    const formData = new FormData();

    if (uploadedFile) {
        formData.append('file', uploadedFile);
    } else if (uploadedFileUrl) {
        formData.append('url', uploadedFileUrl);
    }

    formData.append('chunk_size', chunkSize.value);
    formData.append('chunking_mode', chunkingMode.value);

    const saveLoc = saveLocation.value.trim();
    if (saveLoc) {
        formData.append('save_location', saveLoc);
    }

    // Show loading with processing steps
    const steps = [
        { name: 'Uploading file', status: 'pending' },
        { name: 'Converting document', status: 'pending' },
        { name: 'Chunking content', status: 'pending' },
        { name: 'Creating vector database', status: 'pending' }
    ];
    showLoading('Processing Document', steps);

    const startTime = Date.now();

    try {
        // Start SSE connection for progress updates
        const eventSource = new EventSource(`${API_BASE}/upload-progress`);

        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            const progress = data.progress || 0;
            const message = data.message || 'Processing...';

            // Map progress to steps
            let currentStep = -1;
            if (progress >= 0 && progress < 25) currentStep = 0;      // Uploading
            else if (progress >= 25 && progress < 60) currentStep = 1; // Converting
            else if (progress >= 60 && progress < 75) currentStep = 2; // Chunking
            else if (progress >= 75) currentStep = 3;                  // Indexing

            updateProgress(progress, message, currentStep);

            // Close connection when complete
            if (progress >= 100) {
                eventSource.close();
            }
        };

        eventSource.onerror = (error) => {
            console.error('SSE Error:', error);
            eventSource.close();
        };

        // Start the upload (this triggers progress updates)
        const response = await fetch(`${API_BASE}/upload`, {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        const elapsed = Math.round((Date.now() - startTime) / 1000);

        if (data.success) {
            // Ensure progress shows 100%
            updateProgress(100, `Complete! Processed in ${elapsed}s`, 3);
            await new Promise(resolve => setTimeout(resolve, 800));

            dataLoaded = true;
            const chunkingInfo = chunkingMode.value === 'sentences'
                ? `${chunkSize.value} sentence(s) per chunk`
                : chunkingMode.value === 'paragraphs'
                ? 'Full paragraphs'
                : chunkingMode.value === 'fine_paragraphs'
                ? `Max ${chunkSize.value} words per chunk`
                : `~${chunkSize.value} words per chunk`;

            statusText.textContent = `Loaded: ${data.filename} (${data.row_count} chunks, ${chunkingInfo})`;
            statusText.className = 'status-badge status-loaded';
            clearDataBtn.style.display = 'inline-block';

            hideLoading();

            // Enable tabs and switch to schema
            enableTabs();
            const schemaTab = document.getElementById('schema-tab');
            if (schemaTab) {
                schemaTab.click();
            }

            alert(`✓ Document processed successfully!\n\n${data.row_count} chunks created using ${chunkingMode.value} mode.`);
        } else {
            hideLoading();
            alert('Error: ' + (data.message || 'Unknown error'));
        }
    } catch (error) {
        hideLoading();
        console.error('Upload error:', error);
        alert('Error uploading file: ' + error.message);
    }
}

// Keep old handleFileUpload for backward compatibility, but it now just calls handleProcessDocument

async function handleFileUpload() {
    const file = fileInput.files[0];
    if (!file) return;

    const reuseExisting = document.getElementById('reuseExisting');

    const formData = new FormData();
    formData.append('file', file);
    formData.append('chunk_size', chunkSize.value);
    formData.append('chunking_mode', chunkingMode.value);
    formData.append('reuse_existing', reuseExisting ? reuseExisting.checked : true);

    // Show loading with processing steps
    const steps = [
        { name: 'Uploading file', status: 'pending' },
        { name: 'Converting document', status: 'pending' },
        { name: 'Chunking content', status: 'pending' },
        { name: 'Creating vector database', status: 'pending' }
    ];
    showLoading('Processing Document', steps);

    const startTime = Date.now();

    try {
        // Start SSE connection for progress updates
        const eventSource = new EventSource(`${API_BASE}/upload-progress`);

        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            const progress = data.progress || 0;
            const message = data.message || 'Processing...';

            // Map progress to steps
            let currentStep = -1;
            if (progress >= 0 && progress < 25) currentStep = 0;      // Uploading
            else if (progress >= 25 && progress < 60) currentStep = 1; // Converting
            else if (progress >= 60 && progress < 75) currentStep = 2; // Chunking
            else if (progress >= 75) currentStep = 3;                  // Indexing

            updateProgress(progress, message, currentStep);

            // Close connection when complete
            if (progress >= 100) {
                eventSource.close();
            }
        };

        eventSource.onerror = (error) => {
            console.error('SSE Error:', error);
            eventSource.close();
        };

        // Start the upload (this triggers progress updates)
        const response = await fetch(`${API_BASE}/upload`, {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        const elapsed = Math.round((Date.now() - startTime) / 1000);

        if (data.success) {
            // Ensure progress shows 100%
            updateProgress(100, `Complete! Processed in ${elapsed}s`, 3);
            await new Promise(resolve => setTimeout(resolve, 800));

            dataLoaded = true;
            const chunkingInfo = chunkingMode.value === 'sentences'
                ? `${chunkSize.value} sentence(s) per chunk`
                : chunkingMode.value === 'paragraphs'
                ? 'paragraph-level chunking'
                : `~${chunkSize.value} words per chunk`;
            statusText.textContent = `Loaded: ${data.filename} (${data.row_count} rows, ${chunkingInfo})`;
            statusText.className = 'status-badge status-loaded';
            clearDataBtn.style.display = 'inline-block';

            // Enable tabs
            enableTabs();

            // Switch to entity extraction tab (with null check)
            const schemaTab = document.getElementById('schema-tab');
            if (schemaTab) {
                schemaTab.click();
            }

            alert(`✓ ${data.message}`);
        } else {
            alert(`✗ Upload failed: ${data.error || 'Unknown error'}`);
        }
    } catch (error) {
        alert(`✗ Error: ${error.message}`);
    } finally {
        hideLoading();
    }
}

async function handleExtract() {
    if (!currentEntity || !dataLoaded) {
        console.log('Cannot extract: currentEntity=', currentEntity, 'dataLoaded=', dataLoaded);
        return;
    }

    console.log('Extracting infobox for:', currentEntity);

    // Define extraction steps
    const steps = [
        { name: 'Searching for relevant documents', status: 'pending' },
        { name: 'Extracting semantic relations', status: 'pending' },
        { name: 'Aggregating infobox', status: 'pending' }
    ];
    showLoading('Extracting Infobox', steps);
    infoboxResults.innerHTML = '';

    try {
        // Get num_documents value
        const numDocs = numDocsSlider ? numDocsSlider.value : 10;

        // Use Server-Sent Events for progress updates
        const eventSource = new EventSource(`${API_BASE}/extract-stream?entity=${encodeURIComponent(currentEntity)}&num_documents=${numDocs}`);

        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            console.log('SSE update:', data);

            if (data.status === 'progress') {
                // Update progress bar and message based on progress percentage
                const percent = data.progress || 0;
                const message = data.message || 'Processing...';

                // Determine which step we're on based on progress
                let currentStep = -1;
                if (percent >= 25 && percent < 50) {
                    currentStep = 0; // Search in progress
                } else if (percent >= 50 && percent < 75) {
                    currentStep = 1; // Extracting relations
                } else if (percent >= 75) {
                    currentStep = 2; // Aggregating infobox
                }

                updateProgress(percent, message, currentStep);

            } else if (data.status === 'complete') {
                // Close the event source
                eventSource.close();

                // Show 100% completion
                updateProgress(100, 'Extraction complete!', 2);

                // Wait a moment before hiding loading
                setTimeout(() => {
                    hideLoading();
                    // Display the result with triples
                    displayInfobox(data.result, data.triples, data.num_triples, data.search_results, data.num_documents_used);

                    // Automatically switch to Infobox Extraction tab
                    const extractTab = document.getElementById('extract-tab');
                    if (extractTab) {
                        extractTab.click();
                    }
                }, 500);

            } else if (data.status === 'error') {
                eventSource.close();
                hideLoading();
                infoboxResults.innerHTML = `<div class="alert alert-danger">Error: ${data.message}</div>`;
            }
        };

        eventSource.onerror = (error) => {
            console.error('SSE error:', error);
            eventSource.close();
            hideLoading();
            infoboxResults.innerHTML = `<div class="alert alert-danger">Connection error during extraction</div>`;
        };

    } catch (error) {
        console.error('Extract error:', error);
        infoboxResults.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
        hideLoading();
    }
}


function displayInfobox(infobox, triples, numTriples, searchResults, numDocuments) {
    console.log('Displaying infobox:', infobox);
    console.log('Triples:', triples, 'Count:', numTriples);
    console.log('Search results:', searchResults, 'Num documents:', numDocuments);

    if (!infobox) {
        infoboxResults.innerHTML = `<div class="alert alert-warning">No infobox data to display</div>`;
        return;
    }

    try {
        // Parse nested JSON strings
        const processedInfobox = processNestedJSON(infobox);

        // Build triples HTML if available
        let triplesHTML = '';
        if (triples && triples.length > 0) {
            const triplesTable = triples.map((triple, idx) => `
                <tr>
                    <td>${idx + 1}</td>
                    <td>
                        <div class="d-flex align-items-center gap-2">
                            <span class="badge bg-info">${triple.subject || 'N/A'}</span>
                            ${triple.subject && triple.subject !== 'N/A' ? `
                                <button class="btn btn-sm btn-outline-primary"
                                        onclick="generateInfoboxForEntity('${triple.subject.replace(/'/g, "\\'")}')"
                                        title="Generate infobox for ${triple.subject}">
                                    🔍
                                </button>
                            ` : ''}
                        </div>
                    </td>
                    <td><span class="badge bg-success">${triple.relation || 'N/A'}</span></td>
                    <td>
                        <div class="d-flex align-items-center gap-2">
                            <span class="badge bg-warning">${triple.object || 'N/A'}</span>
                            ${triple.object && triple.object !== 'N/A' ? `
                                <button class="btn btn-sm btn-outline-primary"
                                        onclick="generateInfoboxForEntity('${triple.object.replace(/'/g, "\\'")}')"
                                        title="Generate infobox for ${triple.object}">
                                    🔍
                                </button>
                            ` : ''}
                        </div>
                    </td>
                </tr>
            `).join('');

            triplesHTML = `
                <div class="card mt-3">
                    <div class="card-header">
                        <h5 class="mb-0">🔗 Extracted Triples (${numTriples || triples.length})</h5>
                        <small class="text-muted">Click 🔍 to generate infobox for any entity</small>
                    </div>
                    <div class="card-body">
                        <div class="table-responsive" style="max-height: 400px; overflow-y: auto;">
                            <table class="table table-sm table-hover">
                                <thead class="table-light sticky-top">
                                    <tr>
                                        <th style="width: 50px;">#</th>
                                        <th>Subject</th>
                                        <th>Relation</th>
                                        <th>Object</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${triplesTable}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            `;
        }

        // Build search results HTML if available
        let searchResultsHTML = '';
        if (searchResults && searchResults.length > 0) {
            const searchCards = searchResults.map((passage, idx) => `
                <div class="passage-card">
                    <strong>Document ${idx + 1}</strong>
                    <div class="mt-2">${passage.text}</div>
                </div>
            `).join('');

            searchResultsHTML = `
                <div class="card mt-3">
                    <div class="card-header">
                        <h5 class="mb-0">📄 Source Documents (${numDocuments || searchResults.length})</h5>
                        <small class="text-muted">Documents used for infobox extraction</small>
                    </div>
                    <div class="card-body">
                        ${searchCards}
                    </div>
                </div>
            `;
        }

        infoboxResults.innerHTML = `
            <div class="alert alert-success">
                ✅ Infobox extraction complete for: <strong>${infobox.entity || 'Unknown'}</strong>
            </div>
            <div class="row">
                <div class="col-md-12">
                    ${renderWikipediaStyleInfobox(processedInfobox, infobox.entity || currentEntity)}
                </div>
            </div>
            ${triplesHTML}
            ${searchResultsHTML}
            <div class="mt-3">
                <button class="btn btn-secondary" onclick="downloadJSON()">
                    💾 Download Infobox JSON
                </button>
                ${triples && triples.length > 0 ? `
                <button class="btn btn-secondary ms-2" onclick="downloadTriples()">
                    💾 Download Triples JSON
                </button>
                ` : ''}
            </div>
        `;

        window.currentInfobox = processedInfobox;
        window.currentTriples = triples;
    } catch (error) {
        console.error('Error displaying infobox:', error);
        infoboxResults.innerHTML = `<div class="alert alert-danger">Error displaying infobox: ${error.message}</div>`;
    }
}
// Function to generate infobox for a specific entity from the schema extraction results
async function generateInfoboxForEntity(entity) {
    if (!entity || !dataLoaded) {
        alert('Cannot generate infobox: no entity or data not loaded');
        return;
    }

    console.log('Generating infobox for entity:', entity);

    // Update current entity
    currentEntity = entity;

    // Update the entity input field to show what we're extracting
    if (entityInput) {
        entityInput.value = entity;
    }

    // Trigger extraction directly
    await handleExtract();
}
// Function to render infobox in Wikipedia style
function renderWikipediaStyleInfobox(infobox, entityName) {
    if (!infobox || typeof infobox !== 'object') {
        return '<p class="text-muted">No infobox data available</p>';
    }

    // Parse the infobox JSON string if needed
    let infoboxData = infobox;
    if (typeof infobox.infobox === 'string') {
        try {
            infoboxData = JSON.parse(infobox.infobox);
        } catch (e) {
            infoboxData = infobox;
        }
    } else if (infobox.infobox && typeof infobox.infobox === 'object') {
        infoboxData = infobox.infobox;
    }

    // Helper function to format values
    function formatValue(value) {
        if (value === null || value === undefined) {
            return '<span class="text-muted fst-italic">Not available</span>';
        } else if (Array.isArray(value)) {
            if (value.length === 0) {
                return '<span class="text-muted fst-italic">Empty list</span>';
            }
            // Check if array contains objects
            if (typeof value[0] === 'object' && value[0] !== null) {
                // Render as a nested table for array of objects
                const keys = Object.keys(value[0]);
                let tableHtml = '<table class="table table-sm table-bordered mt-1 mb-0"><thead><tr>';
                keys.forEach(k => {
                    tableHtml += `<th class="text-capitalize">${k.replace(/_/g, ' ')}</th>`;
                });
                tableHtml += '</tr></thead><tbody>';
                value.forEach(item => {
                    tableHtml += '<tr>';
                    keys.forEach(k => {
                        const cellValue = item[k];
                        tableHtml += `<td>${cellValue !== null && cellValue !== undefined ? cellValue : '-'}</td>`;
                    });
                    tableHtml += '</tr>';
                });
                tableHtml += '</tbody></table>';
                return tableHtml;
            } else {
                // Simple array - render as list
                return value.map(item => `<div class="infobox-list-item">• ${item}</div>`).join('');
            }
        } else if (typeof value === 'object') {
            return `<pre class="infobox-json">${JSON.stringify(value, null, 2)}</pre>`;
        } else {
            return String(value);
        }
    }

    let html = `
        <div class="wikipedia-infobox">
            <div class="infobox-title">${entityName || infobox.entity || 'Entity'}</div>
            <table class="infobox-table">
    `;

    // Render each field in the infobox
    for (const [key, value] of Object.entries(infoboxData)) {
        if (key === 'entity') continue; // Skip entity field as it's in the title

        // Format key name (convert snake_case to Title Case)
        const formattedKey = key
            .replace(/_/g, ' ')
            .replace(/\b\w/g, l => l.toUpperCase());

        html += `
            <tr>
                <th class="infobox-label">${formattedKey}</th>
                <td class="infobox-value">${formatValue(value)}</td>
            </tr>
        `;
    }

    html += `
            </table>
        </div>
    `;

    return html;
}



function processNestedJSON(obj) {
    if (typeof obj === 'string') {
        try {
            return processNestedJSON(JSON.parse(obj));
        } catch {
            return obj;
        }
    } else if (Array.isArray(obj)) {
        return obj.map(item => processNestedJSON(item));
    } else if (obj && typeof obj === 'object') {
        const result = {};
        for (const [key, value] of Object.entries(obj)) {
            result[key] = processNestedJSON(value);
        }
        return result;
    }
    return obj;
}

function renderFoldableJSON(obj, level = 0) {
    if (obj === null || obj === undefined) {
        return `<span class="text-muted">null</span>`;
    }

    if (typeof obj !== 'object') {
        const className = typeof obj === 'string' ? 'text-success' : 'text-info';
        const value = typeof obj === 'string' ? `"${obj}"` : obj;
        return `<span class="${className}">${value}</span>`;
    }

    if (Array.isArray(obj)) {
        if (obj.length === 0) return '<span class="text-muted">[]</span>';

        const items = obj.map((item, idx) => `
            <div style="margin-left: ${(level + 1) * 20}px">
                <strong>${idx}:</strong> ${renderFoldableJSON(item, level + 1)}
            </div>
        `).join('');

        return `
            <details open style="margin-left: ${level * 20}px">
                <summary class="text-primary" style="cursor: pointer;">
                    <strong>Array (${obj.length} items)</strong>
                </summary>
                ${items}
            </details>
        `;
    }

    // Object
    const entries = Object.entries(obj);
    if (entries.length === 0) return '<span class="text-muted">{}</span>';

    const items = entries.map(([key, value]) => `
        <div style="margin-left: ${(level + 1) * 20}px; margin-top: 5px;">
            <strong class="text-primary">${key}:</strong> ${renderFoldableJSON(value, level + 1)}
        </div>
    `).join('');

    return `
        <details open style="margin-left: ${level * 20}px">
            <summary class="text-primary" style="cursor: pointer;">
                <strong>Object (${entries.length} fields)</strong>
            </summary>
            ${items}
        </details>
    `;
}

function downloadJSON() {
    const blob = new Blob([JSON.stringify(window.currentInfobox, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `infobox_${currentEntity}.json`;
    link.click();
    URL.revokeObjectURL(url);
}

function downloadTriples() {
    if (!window.currentTriples) {
        alert('No triples data available');
        return;
    }
    const blob = new Blob([JSON.stringify(window.currentTriples, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `triples_${currentEntity}.json`;
    link.click();
    URL.revokeObjectURL(url);
}

function updateExtractButton() {
    if (currentEntity && dataLoaded) {
        extractBtn.disabled = false;
        extractInfo.innerHTML = `<p class="mb-0">Ready to extract infobox for: <strong>${currentEntity}</strong></p>`;
    } else {
        extractBtn.disabled = true;
        extractInfo.innerHTML = '<p class="mb-0">Search for an entity first</p>';
    }
}

let progressSteps = [];

function showLoading(title = 'Processing', steps = []) {
    document.getElementById('loadingTitle').textContent = title;
    document.getElementById('loadingText').textContent = 'Initializing...';
    document.getElementById('progressFill').style.width = '0%';
    document.getElementById('progressPercent').textContent = '0%';

    progressSteps = steps;
    updateLoadingSteps();

    loadingOverlay.style.display = 'flex';
}

function updateProgress(percent, status, currentStep = -1) {
    document.getElementById('progressFill').style.width = `${percent}%`;
    document.getElementById('progressPercent').textContent = `${Math.round(percent)}%`;
    document.getElementById('loadingText').textContent = status;

    if (currentStep >= 0 && currentStep < progressSteps.length) {
        // Mark previous steps as completed
        for (let i = 0; i < currentStep; i++) {
            progressSteps[i].status = 'completed';
        }
        // Mark current step as active
        progressSteps[currentStep].status = 'active';
        // Mark future steps as pending
        for (let i = currentStep + 1; i < progressSteps.length; i++) {
            progressSteps[i].status = 'pending';
        }
        updateLoadingSteps();
    }
}

function updateLoadingSteps() {
    const stepsContainer = document.getElementById('loadingSteps');
    stepsContainer.innerHTML = '';

    progressSteps.forEach(step => {
        const stepDiv = document.createElement('div');
        stepDiv.className = `step-item ${step.status || 'pending'}`;

        let icon = '⏳';
        if (step.status === 'completed') icon = '✅';
        else if (step.status === 'active') icon = '🔄';

        stepDiv.innerHTML = `
            <span class="step-icon">${icon}</span>
            <span class="step-name">${step.name}</span>
        `;

        stepsContainer.appendChild(stepDiv);
    });
}

function animateProgress(startPercent, endPercent, duration) {
    return new Promise(resolve => {
        const startTime = Date.now();
        const percentDiff = endPercent - startPercent;

        function animate() {
            const elapsed = Date.now() - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const currentPercent = startPercent + (percentDiff * progress);

            document.getElementById('progressFill').style.width = `${currentPercent}%`;
            document.getElementById('progressPercent').textContent = `${Math.round(currentPercent)}%`;

            if (progress < 1) {
                requestAnimationFrame(animate);
            } else {
                resolve();
            }
        }

        animate();
    });
}

function hideLoading() {
    loadingOverlay.style.display = 'none';
}

// ============================================================================
// SCHEMA BUILDER FUNCTIONS
// ============================================================================

function addSchemaField() {
    const fieldId = fieldIdCounter++;
    const field = {
        id: fieldId,
        name: '',
        type: 'str',
        description: '',
        required: false  // All fields are optional by default
    };
    schemaFields.push(field);
    renderSchemaFields();
    updateSchemaButtons();
}

function removeSchemaField(fieldId) {
    schemaFields = schemaFields.filter(f => f.id !== fieldId);
    renderSchemaFields();
    updateSchemaButtons();
}

function renderSchemaFields() {
    if (schemaFields.length === 0) {
        schemaFieldsBody.innerHTML = `
            <tr>
                <td colspan="4" class="text-center text-muted">
                    No fields added yet. Click "Add Field" to start building your schema.
                </td>
            </tr>
        `;
        return;
    }

    schemaFieldsBody.innerHTML = schemaFields.map(field => `
        <tr data-field-id="${field.id}">
            <td>
                <input type="text" class="form-control form-control-sm"
                       value="${field.name}"
                       placeholder="field_name"
                       onchange="updateFieldProperty(${field.id}, 'name', this.value)">
            </td>
            <td>
                <select class="form-select form-select-sm"
                        onchange="updateFieldProperty(${field.id}, 'type', this.value)">
                    <option value="str" ${field.type === 'str' ? 'selected' : ''}>str</option>
                    <option value="int" ${field.type === 'int' ? 'selected' : ''}>int</option>
                    <option value="float" ${field.type === 'float' ? 'selected' : ''}>float</option>
                    <option value="bool" ${field.type === 'bool' ? 'selected' : ''}>bool</option>
                    <option value="List[str]" ${field.type === 'List[str]' ? 'selected' : ''}>List[str]</option>
                </select>
            </td>
            <td>
                <input type="text" class="form-control form-control-sm"
                       value="${field.description}"
                       placeholder="Field description"
                       onchange="updateFieldProperty(${field.id}, 'description', this.value)">
            </td>
            <td class="text-center">
                <button class="btn btn-sm btn-outline-danger" onclick="removeSchemaField(${field.id})">
                    ✕
                </button>
            </td>
        </tr>
    `).join('');
}

function updateFieldProperty(fieldId, property, value) {
    const field = schemaFields.find(f => f.id === fieldId);
    if (field) {
        field[property] = value;
        updateSchemaButtons();
    }
}

function updateSchemaButtons() {
    const hasName = schemaName.value.trim().length > 0;
    const hasFields = schemaFields.length > 0 && schemaFields.every(f => f.name.trim().length > 0);
    const isValid = hasName && hasFields && dataLoaded;

    extractSchemaBtn.disabled = !isValid;
    saveSchemaBtn.disabled = !hasName || !hasFields;
}

function handleSchemaClear() {
    if (confirm('Clear the current schema? This will remove all fields.')) {
        schemaName.value = '';
        schemaInstructions.value = '';
        if (schemaAsList) schemaAsList.checked = false;
        schemaFields = [];
        fieldIdCounter = 0;
        renderSchemaFields();
        updateSchemaButtons();
        schemaResults.innerHTML = '';
    }
}

function loadTemplate(templateName) {
    // Clear existing schema
    schemaFields = [];
    fieldIdCounter = 0;

    // Set schema name
    schemaName.value = templateName;

    // Define templates (all fields are optional by default)
    const templates = {
        'Keyword': {
            instructions: 'Extract keywords or key terms from the document',
            asList: true,
            fields: [
                { name: 'keyword', type: 'str', description: 'A keyword or key term', required: false }
            ]
        },
        'Entity': {
            instructions: 'Extract named entities from the document (people, organizations, locations, concepts, etc.)',
            asList: true,
            fields: [
                { name: 'name', type: 'str', description: 'Name of the entity', required: false },
                { name: 'type', type: 'str', description: 'Type of entity (e.g., Person, Organization, Location, Concept, Event)', required: false }
            ]
        }
    };

    const template = templates[templateName];
    if (!template) return;

    // Set instructions
    schemaInstructions.value = template.instructions;

    // Set extract as list
    if (schemaAsList) {
        schemaAsList.checked = template.asList;
    }

    // Add fields
    template.fields.forEach(fieldDef => {
        const fieldId = fieldIdCounter++;
        schemaFields.push({
            id: fieldId,
            name: fieldDef.name,
            type: fieldDef.type,
            description: fieldDef.description,
            required: fieldDef.required
        });
    });

    // Render and update
    renderSchemaFields();
    updateSchemaButtons();

    // Show success message
    const alert = document.createElement('div');
    alert.className = 'alert alert-success alert-dismissible fade show mt-2';
    alert.innerHTML = `
        <strong>✓ Template loaded:</strong> ${templateName} schema with ${template.fields.length} field(s)
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    schemaName.parentElement.appendChild(alert);
    setTimeout(() => alert.remove(), 3000);
}

function handleSchemaSave() {
    const name = schemaName.value.trim();
    if (!name) {
        alert('Please enter a schema name');
        return;
    }

    if (schemaFields.length === 0) {
        alert('Please add at least one field');
        return;
    }

    const invalidFields = schemaFields.filter(f => !f.name.trim());
    if (invalidFields.length > 0) {
        alert('All fields must have a name');
        return;
    }

    const schema = {
        name: name,
        fields: schemaFields.map(f => ({
            name: f.name,
            type: f.type,
            description: f.description,
            required: f.required
        })),
        instructions: schemaInstructions.value.trim(),
        as_list: schemaAsList ? schemaAsList.checked : false
    };

    // Download as JSON
    const blob = new Blob([JSON.stringify(schema, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `schema_${name}.json`;
    link.click();
    URL.revokeObjectURL(url);

    alert(`Schema "${name}" saved successfully!`);
}

async function handleSchemaExtract() {
    const name = schemaName.value.trim();
    if (!name) {
        alert('Please enter a schema name');
        return;
    }

    if (schemaFields.length === 0) {
        alert('Please add at least one field');
        return;
    }

    const invalidFields = schemaFields.filter(f => !f.name.trim());
    if (invalidFields.length > 0) {
        alert('All fields must have a name');
        return;
    }

    if (!dataLoaded) {
        alert('Please upload a document first');
        return;
    }

    // Prepare schema data
    const schemaData = {
        name: name,
        fields: schemaFields.map(f => ({
            name: f.name,
            type: f.type,
            description: f.description,
            required: f.required
        })),
        instructions: schemaInstructions.value.trim(),
        as_list: schemaAsList ? schemaAsList.checked : false
    };

    // Show loading with extraction steps
    const steps = [
        { name: 'Creating dynamic schema', status: 'pending' },
        { name: 'Searching relevant content', status: 'pending' },
        { name: 'Extracting structured data', status: 'pending' },
        { name: 'Resolving coreferences', status: 'pending' },
        { name: 'Consolidating results', status: 'pending' }
    ];
    showLoading('Extracting Schema Data', steps);
    schemaResults.innerHTML = '';

    try {
        // Use Server-Sent Events for progress updates
        const eventSource = new EventSource(
            `${API_BASE}/schema/extract-stream?` +
            `schema_name=${encodeURIComponent(name)}&` +
            `fields=${encodeURIComponent(JSON.stringify(schemaData.fields))}&` +
            `instructions=${encodeURIComponent(schemaData.instructions || '')}&` +
            `as_list=${schemaData.as_list}`
        );

        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            console.log('Schema extraction SSE update:', data);

            if (data.status === 'progress') {
                const percent = data.progress || 0;
                const message = data.message || 'Processing...';

                // Determine which step we're on (updated for 5 steps)
                let currentStep = -1;
                if (percent >= 10 && percent < 35) currentStep = 0;      // Creating schema
                else if (percent >= 35 && percent < 60) currentStep = 1; // Searching
                else if (percent >= 60 && percent < 75) currentStep = 2; // Extracting
                else if (percent >= 75 && percent < 90) currentStep = 3; // Resolving coreferences
                else if (percent >= 90) currentStep = 4;                 // Consolidating

                updateProgress(percent, message, currentStep);

            } else if (data.status === 'complete') {
                eventSource.close();
                updateProgress(100, 'Extraction complete!', 4);

                setTimeout(() => {
                    hideLoading();
                    displaySchemaResults(data.results, name);
                }, 500);

            } else if (data.status === 'error') {
                eventSource.close();
                hideLoading();
                schemaResults.innerHTML = `<div class="alert alert-danger">Error: ${data.message}</div>`;
            }
        };

        eventSource.onerror = (error) => {
            console.error('Schema extraction SSE error:', error);
            eventSource.close();
            hideLoading();
            schemaResults.innerHTML = `<div class="alert alert-danger">Connection error during extraction</div>`;
        };

    } catch (error) {
        console.error('Schema extraction error:', error);
        hideLoading();
        schemaResults.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

function displaySchemaResults(results, schemaName) {
    if (!results || results.length === 0) {
        schemaResults.innerHTML = `
            <div class="alert alert-warning">
                No data extracted. Try adjusting your schema or instructions.
            </div>
        `;
        return;
    }

    schemaResults.innerHTML = `
        <div class="alert alert-success">
            ✅ Successfully extracted ${results.length} record(s) using schema: <strong>${schemaName}</strong>
        </div>
        <div class="card">
            <div class="card-header d-flex justify-content-between align-items-center">
                <strong>Extracted Data</strong>
                <div>
                    <button class="btn btn-sm btn-outline-primary" onclick="downloadSchemaResults('json')">
                        💾 Download JSON
                    </button>
                    <button class="btn btn-sm btn-outline-success" onclick="downloadSchemaResults('csv')">
                        📊 Download CSV
                    </button>
                </div>
            </div>
            <div class="card-body">
                ${renderSchemaResultsTable(results)}
            </div>
        </div>
    `;

    // Store results globally for download
    window.currentSchemaResults = results;
    window.currentSchemaName = schemaName;
}

function renderSchemaResultsTable(results) {
    if (results.length === 0) return '<p class="text-muted">No results</p>';

    // Get all unique keys from all results
    const allKeys = new Set();
    results.forEach(result => {
        Object.keys(result).forEach(key => allKeys.add(key));
    });
    const keys = Array.from(allKeys);

    // Store results for sorting
    window.schemaTableData = {
        results: results,
        keys: keys,
        sortColumn: null,
        sortDirection: 'asc'
    };

    let html = `
        <div class="table-responsive">
            <table class="table table-striped table-hover" id="schemaResultsTable">
                <thead class="table-light">
                    <tr>
                        <th style="cursor: pointer;" onclick="sortSchemaTable(-1)">
                            # <span id="sort-icon--1"></span>
                        </th>
                        ${keys.map((key, idx) => `
                            <th style="cursor: pointer;" onclick="sortSchemaTable(${idx})" title="Click to sort">
                                ${key} <span id="sort-icon-${idx}"></span>
                            </th>
                        `).join('')}
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody id="schemaResultsTableBody">
    `;

    results.forEach((result, idx) => {
        html += `<tr><td>${idx + 1}</td>`;

        // Store entity value for the action button
        let entityValue = null;

        keys.forEach(key => {
            const value = result[key];
            let displayValue = '';

            if (value === null || value === undefined) {
                displayValue = '<span class="text-muted">null</span>';
            } else if (Array.isArray(value)) {
                displayValue = `<span class="badge bg-info">${value.length} items</span> ${JSON.stringify(value)}`;
            } else if (typeof value === 'object') {
                displayValue = `<details><summary>Object</summary><pre>${JSON.stringify(value, null, 2)}</pre></details>`;
            } else {
                displayValue = String(value);
                // Store the first non-null string value as potential entity
                if (!entityValue && typeof value === 'string' && value.trim()) {
                    entityValue = value;
                }
            }

            html += `<td>${displayValue}</td>`;
        });

        // Add action button column
        if (entityValue) {
            html += `
                <td>
                    <button class="btn btn-sm btn-primary"
                            onclick="generateInfoboxForEntity('${entityValue.replace(/'/g, "\\'")}')"
                            title="Generate infobox for ${entityValue}">
                        🔍 Generate Infobox
                    </button>
                </td>
            `;
        } else {
            html += `<td><span class="text-muted">-</span></td>`;
        }

        html += `</tr>`;
    });

    html += `
                </tbody>
            </table>
        </div>
    `;

    return html;
}

function sortSchemaTable(columnIndex) {
    const data = window.schemaTableData;
    if (!data) return;

    const { results, keys } = data;

    // Toggle sort direction if clicking same column
    if (data.sortColumn === columnIndex) {
        data.sortDirection = data.sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        data.sortColumn = columnIndex;
        data.sortDirection = 'asc';
    }

    // Sort the results
    const sortedResults = [...results].sort((a, b) => {
        let aVal, bVal;

        if (columnIndex === -1) {
            // Sort by row number (original index)
            aVal = results.indexOf(a);
            bVal = results.indexOf(b);
        } else {
            const key = keys[columnIndex];
            aVal = a[key];
            bVal = b[key];
        }

        // Handle null/undefined
        if (aVal === null || aVal === undefined) return 1;
        if (bVal === null || bVal === undefined) return -1;

        // Handle arrays
        if (Array.isArray(aVal)) aVal = aVal.length;
        if (Array.isArray(bVal)) bVal = bVal.length;

        // Handle objects
        if (typeof aVal === 'object') aVal = JSON.stringify(aVal);
        if (typeof bVal === 'object') bVal = JSON.stringify(bVal);

        // Compare
        if (aVal < bVal) return data.sortDirection === 'asc' ? -1 : 1;
        if (aVal > bVal) return data.sortDirection === 'asc' ? 1 : -1;
        return 0;
    });

    // Update the table body
    const tbody = document.getElementById('schemaResultsTableBody');
    if (!tbody) return;

    let html = '';
    sortedResults.forEach((result, idx) => {
        html += `<tr><td>${idx + 1}</td>`;

        // Store entity value for the action button
        let entityValue = null;

        keys.forEach(key => {
            const value = result[key];
            let displayValue = '';

            if (value === null || value === undefined) {
                displayValue = '<span class="text-muted">null</span>';
            } else if (Array.isArray(value)) {
                displayValue = `<span class="badge bg-info">${value.length} items</span> ${JSON.stringify(value)}`;
            } else if (typeof value === 'object') {
                displayValue = `<details><summary>Object</summary><pre>${JSON.stringify(value, null, 2)}</pre></details>`;
            } else {
                displayValue = String(value);
                // Store the first non-null string value as potential entity
                if (!entityValue && typeof value === 'string' && value.trim()) {
                    entityValue = value;
                }
            }

            html += `<td>${displayValue}</td>`;
        });

        // Add action button column
        if (entityValue) {
            html += `
                <td>
                    <button class="btn btn-sm btn-primary"
                            onclick="generateInfoboxForEntity('${entityValue.replace(/'/g, "\\'")}')"
                            title="Generate infobox for ${entityValue}">
                        🔍 Generate Infobox
                    </button>
                </td>
            `;
        } else {
            html += `<td><span class="text-muted">-</span></td>`;
        }

        html += `</tr>`;
    });

    tbody.innerHTML = html;

    // Update sort icons
    document.querySelectorAll('[id^="sort-icon-"]').forEach(el => {
        el.textContent = '';
    });

    const icon = document.getElementById(`sort-icon-${columnIndex}`);
    if (icon) {
        icon.textContent = data.sortDirection === 'asc' ? ' ▲' : ' ▼';
    }
}

function downloadSchemaResults(format) {
    const results = window.currentSchemaResults;
    const schemaName = window.currentSchemaName;

    if (!results || results.length === 0) {
        alert('No results to download');
        return;
    }

    if (format === 'json') {
        const blob = new Blob([JSON.stringify(results, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${schemaName}_results.json`;
        link.click();
        URL.revokeObjectURL(url);
    } else if (format === 'csv') {
        // Convert to CSV
        const allKeys = new Set();
        results.forEach(result => {
            Object.keys(result).forEach(key => allKeys.add(key));
        });
        const keys = Array.from(allKeys);

        let csv = keys.join(',') + '\n';
        results.forEach(result => {
            const row = keys.map(key => {
                const value = result[key];
                if (value === null || value === undefined) return '';
                if (Array.isArray(value) || typeof value === 'object') {
                    return '"' + JSON.stringify(value).replace(/"/g, '""') + '"';
                }
                return '"' + String(value).replace(/"/g, '""') + '"';
            });
            csv += row.join(',') + '\n';
        });

        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${schemaName}_results.csv`;
        link.click();
        URL.revokeObjectURL(url);
    }
}

// Make functions globally accessible
window.updateFieldProperty = updateFieldProperty;
window.removeSchemaField = removeSchemaField;
window.downloadSchemaResults = downloadSchemaResults;

// ============================================================================

// Made with Bob
