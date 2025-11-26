import os
import time
import logging
import argparse
from datetime import datetime
import pandas as pd
from pycelonis import get_celonis
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("celonis_push.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_file_extension(file_path):
    return os.path.splitext(file_path)[1].lower()

def read_file(file_path):
    """Reads a file into a pandas DataFrame."""
    ext = get_file_extension(file_path)
    try:
        if ext == '.csv':
            # Assuming standard CSV; might need delimiter config
            return pd.read_csv(file_path)
        elif ext == '.parquet':
            return pd.read_parquet(file_path)
        elif ext in ['.xls', '.xlsx']:
            return pd.read_excel(file_path)
        else:
            logger.error(f"Unsupported file format: {ext} for file {file_path}")
            return None
    except Exception as e:
        logger.error(f"Failed to read file {file_path}: {e}")
        return None

def push_to_celonis(celonis, data_pool_id, file_path):
    """Pushes a single file to Celonis Data Pool."""
    file_name = os.path.basename(file_path)
    table_name = os.path.splitext(file_name)[0]
    
    logger.info(f"Processing file: {file_name} -> Table: {table_name}")
    
    df = read_file(file_path)
    if df is None:
        return

    record_count = len(df)
    current_time = datetime.now().isoformat()
    
    try:
        # Get Data Pool
        data_pool = celonis.data_integration.get_data_pool(data_pool_id)
        
        # 1. Ensure Data Job exists
        job_name = "TEST_DATA_JOB"
        try:
            data_job = data_pool.get_jobs().find(job_name)
            logger.info(f"Found existing Data Job: {job_name}")
        except Exception:
            logger.info(f"Data Job {job_name} not found. Creating...")
            data_job = data_pool.create_job(job_name)
            
        # 2. Generate SQL for Table Creation
        # Map pandas dtypes to Celonis SQL types
        def get_sql_type(dtype):
            if pd.api.types.is_integer_dtype(dtype): return "INT"
            if pd.api.types.is_float_dtype(dtype): return "FLOAT"
            if pd.api.types.is_datetime64_any_dtype(dtype): return "TIMESTAMP"
            return "VARCHAR(2000)" # Safe default

        columns_sql = []
        for col, dtype in df.dtypes.items():
            col_name = col.replace(" ", "_") # Sanitize column names
            sql_type = get_sql_type(dtype)
            columns_sql.append(f'"{col_name}" {sql_type}')
        
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  {", ".join(columns_sql)}\n);'
        logger.info(f"Generated SQL: {create_table_sql}")

        # 3. Ensure Transformation exists and run SQL
        trans_name = "TEST_TRANSFORMATION"
        try:
            # Check if transformation exists in the JOB
            transformations = data_job.get_transformations()
            transformation = next((t for t in transformations if t.name == trans_name), None)
            
            if transformation:
                logger.info(f"Found existing Transformation: {trans_name}. Recreating with new SQL...")
                transformation.delete()
                transformation = data_job.create_transformation(trans_name, statement=create_table_sql)
            else:
                logger.info(f"Transformation {trans_name} not found. Creating...")
                transformation = data_job.create_transformation(trans_name, statement=create_table_sql)
                
            # Execute Transformation via Data Job
            logger.info(f"Executing Data Job {job_name} to run transformation...")
            # In pycelonis 2.x, we might need to trigger the job. 
            # Assuming execute() exists on job based on inspection.
            data_job.execute()
            time.sleep(10) # Wait for job to likely finish (async)
            
        except Exception as e:
            logger.error(f"Failed to handle transformation: {e}")
            # Continue to ingestion? If table creation failed, ingestion might fail if table doesn't exist.
            # But we have fallback in ingestion logic.

        # 4. Ingest Data (Chunking)
        CHUNK_SIZE = 100000
        total_records = len(df)
        
        if total_records <= CHUNK_SIZE:
            # Small file: push all at once
            try:
                table = data_pool.get_table(table_name)
                logger.info(f"Table {table_name} exists. Appending data...")
                table.append(df)
            except Exception:
                # Fallback if SQL failed
                logger.info(f"Table {table_name} does not exist (append failed). Creating via push...")
                data_pool.create_table(df, table_name)
                
            log_entry = (
                f"SUCCESS | Source: {file_name} | Records: {total_records} | "
                f"Time: {current_time} | Pool ID: {data_pool_id} | "
                f"Inserted: {total_records}"
            )
            logger.info(log_entry)
            
        else:
            # Large file: chunking
            num_chunks = (total_records + CHUNK_SIZE - 1) // CHUNK_SIZE
            logger.info(f"File has {total_records} records. Splitting into {num_chunks} chunks of {CHUNK_SIZE}.")
            
            for i in range(num_chunks):
                start_idx = i * CHUNK_SIZE
                end_idx = min((i + 1) * CHUNK_SIZE, total_records)
                chunk_df = df.iloc[start_idx:end_idx]
                
                logger.info(f"Processing chunk {i+1}/{num_chunks} ({start_idx} to {end_idx})...")
                
                try:
                    # Try to append first
                    table = data_pool.get_table(table_name)
                    table.append(chunk_df)
                except Exception:
                    # If failed (likely table missing), create it
                    logger.info(f"Table {table_name} not found or append failed. Creating/Upserting...")
                    data_pool.create_table(chunk_df, table_name)
                
                logger.info(f"Chunk {i+1} uploaded successfully.")
                
                # Wait 10 seconds if not the last chunk
                if i < num_chunks - 1:
                    logger.info("Waiting 10 seconds before next batch...")
                    time.sleep(10)
            
            log_entry = (
                f"SUCCESS | Source: {file_name} | Records: {total_records} | "
                f"Time: {current_time} | Pool ID: {data_pool_id} | "
                f"Inserted: {total_records} (in {num_chunks} chunks)"
            )
            logger.info(log_entry)
        
    except Exception as e:
        logger.error(f"Failed to push {file_name} to Celonis: {e}")

def main():
    parser = argparse.ArgumentParser(description="Celonis Data Push Script")
    
    # Arguments are now optional if they exist in .env
    parser.add_argument("--path", help="Path to file or folder (overrides DATA_SOURCE_PATH in .env)")
    parser.add_argument("--api_key", help="Celonis API Key / App Key (overrides CELONIS_API_KEY in .env)")
    parser.add_argument("--instance_id", help="Celonis Instance ID or URL (overrides CELONIS_INSTANCE_ID in .env)")
    parser.add_argument("--pool_id", help="Data Pool ID (overrides CELONIS_POOL_ID in .env)")
    
    args = parser.parse_args()
    
    # Resolve configuration (CLI args > Environment Variables)
    path = args.path or os.getenv("DATA_SOURCE_PATH")
    api_key = args.api_key or os.getenv("CELONIS_API_KEY")
    instance_id = args.instance_id or os.getenv("CELONIS_INSTANCE_ID")
    pool_id = args.pool_id or os.getenv("CELONIS_POOL_ID")
    
    # Validation
    missing_params = []
    if not path: missing_params.append("path")
    if not api_key: missing_params.append("api_key")
    if not instance_id: missing_params.append("instance_id")
    if not pool_id: missing_params.append("pool_id")
    
    if missing_params:
        logger.error(f"Missing required configuration: {', '.join(missing_params)}. Please provide via CLI arguments or .env file.")
        return

    # Construct Base URL if only Instance ID is provided
    base_url = instance_id
    if not base_url.startswith("http"):
        base_url = f"https://{instance_id}.celonis.cloud/"
    
    try:
        # Connect to Celonis
        celonis = get_celonis(base_url=base_url, api_token=api_key)
    except Exception as e:
        logger.error(f"Failed to connect to Celonis: {e}")
        return

    target_path = path
    
    if os.path.isfile(target_path):
        push_to_celonis(celonis, pool_id, target_path)
    elif os.path.isdir(target_path):
        # Get all files
        files = [f for f in os.listdir(target_path) if os.path.isfile(os.path.join(target_path, f))]
        # Filter for supported extensions
        files = [f for f in files if get_file_extension(f) in ['.csv', '.parquet', '.xls', '.xlsx']]
        
        if not files:
            logger.warning("No supported files found in directory.")
            return

        for i, file_name in enumerate(files):
            full_path = os.path.join(target_path, file_name)
            push_to_celonis(celonis, pool_id, full_path)
            
            # Wait 10 seconds if there are more files
            if i < len(files) - 1:
                logger.info("Waiting 10 seconds before next upload...")
                time.sleep(10)
    else:
        logger.error(f"Invalid path: {target_path}")

if __name__ == "__main__":
    main()
