// Common utilities and API configuration for AGstream Manager

// API Configuration
const API_URL = 'http://localhost:5002/api';

// Utility Functions
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showAlert(containerId, message, type) {
    const container = document.getElementById(containerId);
    if (!container) return;

    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type}`;
    alertDiv.textContent = message;
    container.innerHTML = '';
    container.appendChild(alertDiv);

    setTimeout(() => {
        alertDiv.style.opacity = '0';
        setTimeout(() => alertDiv.remove(), 300);
    }, 3000);
}

// Main navigation
function switchMainSection(sectionName) {
    document.querySelectorAll('.main-nav-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.main-section').forEach(sec => sec.classList.remove('active'));

    document.querySelector(`[onclick="switchMainSection('${sectionName}')"]`).classList.add('active');
    document.getElementById(`${sectionName}Section`).classList.add('active');

    // Load data for the section
    if (sectionName === 'schemas') {
        loadSchemaTypes();
    } else if (sectionName === 'transductions') {
        loadTransductions();
    } else if (sectionName === 'topics') {
        loadTopics();
    } else if (sectionName === 'listeners') {
        loadListeners();
    }
}

// Export for use in other modules
window.AGStreamCommon = {
    API_URL,
    escapeHtml,
    showAlert,
    switchMainSection
};

// Made with Bob
