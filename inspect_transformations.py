from pycelonis import get_celonis
from dotenv import load_dotenv
import os

load_dotenv()

celonis = get_celonis(
    base_url=os.getenv("CELONIS_INSTANCE_ID"),
    api_token=os.getenv("CELONIS_API_KEY")
)

pool_id = os.getenv("CELONIS_POOL_ID")
data_pool = celonis.data_integration.get_data_pool(pool_id)

print("Pool attributes:", [a for a in dir(data_pool) if 'trans' in a.lower() or 'task' in a.lower()])

try:
    job = data_pool.get_jobs().find("TEST_DATA_JOB")
    print("Job attributes:", [a for a in dir(job) if 'trans' in a.lower() or 'task' in a.lower()])
except:
    print("Job not found")
