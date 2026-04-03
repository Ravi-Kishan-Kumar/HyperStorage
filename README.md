# HyperStorage 🗄️
**A Decentralized File Storage Platform with AI Classification**

HyperStorage is a secure, distributed storage system designed to eliminate single points of failure. It distributes user data across a network of independent storage nodes while integrating a machine learning classification engine to automatically organize unstructured files without compromising user privacy.

## ✨ Core Features
* **AI-Powered Organization:** Uses a Multinomial Naive Bayes classifier and TF-IDF vectorization to automatically tag files (e.g., `INVOICE`, `CODE`, `IMAGE`) based on metadata upon upload.
* **Decentralized Distribution:** Implements automated file sharding, breaking files into smaller chunks before distributing them across multiple nodes.
* **High Availability:** Features a Replication Factor of 3 and a real-time Heartbeat Monitoring system that detects node failures within 45 seconds.
* **Hybrid Security:** Secures data using XOR-based encryption before distribution, alongside SHA-256 hashing for data integrity verification.

## 🛠️ Tech Stack
* **Backend:** Python 3, Flask
* **Frontend:** HTML5, CSS3, Vanilla JavaScript (ES6+)
* **Database:** SQLite (Relational metadata tracking)
* **AI/ML:** Scikit-learn (TF-IDF, Naive Bayes)

## 🚀 How to Run Locally

### 1. Prerequisites
Ensure you have Python 3.8+ installed on your system.

### 2. Setup
Clone the repository and set up a virtual environment:
```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install required dependencies
pip install flask flask-cors scikit-learn numpy requests
```

## 👥 Team

* Ravi Kishan Kumar
* Sachit Ramesha Gowda