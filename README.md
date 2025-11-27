# 🚀 Celonis Data Push Automation: End-to-End Guide

Welcome! This tool automates the process of uploading your data files (CSV, Excel, Parquet, JSON, XML) directly into the Celonis Execution Management System (EMS). It handles all the technical heavy lifting—creating tables, managing data jobs, and ensuring large files are uploaded smoothly.

---

## 📋 Prerequisites

Before you begin, ensure you have the following:

1.  **Python Installed**: You need Python 3.7 or newer. [Download Python](https://www.python.org/downloads/).
2.  **Celonis Account**: Access to a Celonis team with permissions to Data Integration.
3.  **API Key**: A User or App Key from your Celonis profile.
    *   *Profile -> Settings -> Create API Key*
4.  **Data Pool ID**: The ID of the Data Pool where you want to upload data.
    *   *Data Integration -> Select Pool -> ID is in the URL (e.g., `.../pools/THIS-IS-THE-ID/...`)*
5.  **Network Access**: Ensure **Port 443** is open.
    *   > [!IMPORTANT]
    *   > This script requires a direct connection to Celonis via **Port 443** (HTTPS).
    *   > If your organization uses a **Proxy** that intercepts SSL traffic, this script **will not work** without additional configuration (not covered here). Please ensure you have direct internet access or a proxy bypass for Celonis domains.

---

## 🛠️ Installation

1.  **Download/Clone this Project**: Save the files to a folder on your computer.
2.  **Open a Terminal**: Open Command Prompt (cmd) or PowerShell and navigate to the folder.
3.  **Install Requirements**: Run the following command to install necessary libraries:
    ```bash
    pip install -r requirements.txt
    ```

---

## ⚙️ Configuration (The Important Part)

We use a secure configuration file so you don't have to type your password every time.

1.  **Create/Edit `.env` file**:
    Look for a file named `.env` in the folder. If it doesn't exist, create a new text file and name it `.env`.
2.  **Add your details**:
    Open the file with Notepad or any text editor and paste the following, replacing the values with your own:

    ```env
    # Your Celonis API Key
    CELONIS_API_KEY=your_secret_api_key_here

    # Your Celonis Team URL (include https://)
    CELONIS_INSTANCE_ID=https://your-team.celonis.cloud/

    # The ID of the Data Pool to upload to
    CELONIS_POOL_ID=your_data_pool_uuid_here

    # Path to your file or folder of files
    DATA_SOURCE_PATH=C:\Users\You\Documents\MyData
    ```

    *   **Tip**: For `DATA_SOURCE_PATH`, you can point to a single file (e.g., `C:\Data\sales.csv`) or a whole folder. If you choose a folder, the script will upload **every** supported file inside it!
    *   **Note**: JSON and XML files will be automatically flattened into a tabular format before upload.

---

## ▶️ How to Run

Once configured, running the automation is simple:

1.  Open your terminal in the project folder.
2.  Run the script:
    ```bash
    python celonis_data_push.py
    ```

**That's it!** 

---

## 🔍 What Happens Behind the Scenes?

When you run the script, it performs a smart, multi-step process to ensure your data lands safely in Celonis:

1.  **Connects**: Logs into Celonis using your API Key.
2.  **Reads Data**: Opens your file (CSV, Excel, etc.) and understands its structure (columns and data types).
3.  **Prepares the Destination**:
    *   It checks for a Data Job named `TEST_DATA_JOB`. If missing, it creates it.
    *   It generates a SQL command (`CREATE TABLE ...`) perfectly matched to your file's columns.
    *   It updates a Transformation named `TEST_TRANSFORMATION` with this SQL and **executes it**.
    *   *Why?* This ensures the table exists in Celonis with the correct schema *before* we try to pour data into it.
4.  **Uploads Data**:
    *   **Smart Chunking**: If your file is huge (e.g., 1 million rows), the script splits it into smaller "chunks" (100,000 rows each).
    *   **Polite Uploading**: It uploads one chunk, waits 10 seconds, uploads the next, and so on. This prevents overloading the server and ensures reliability.
5.  **Logs**: Every success, error, or step is recorded in `celonis_push.log` so you can see exactly what happened.

---

## ❓ Troubleshooting

*   **"401 Unauthorized"**: Check your `CELONIS_API_KEY` in the `.env` file.
*   **"File not found"**: Check your `DATA_SOURCE_PATH`. Ensure it's a valid path on your computer.
*   **"Table already exists"**: The script tries to append data if the table exists. If the schema (columns) has changed, you might need to drop the table in Celonis manually or rename your file.
*   **Script freezes?**: It might be waiting during the 10-second delay between chunks. Check the log file!

---

## 📝 Advanced Usage

Want to override settings without changing the `.env` file? You can use command-line arguments:

```bash
python celonis_data_push.py --path "C:\NewData\urgent.csv" --pool_id "different-pool-id"
```

Happy Automating! 🚀
