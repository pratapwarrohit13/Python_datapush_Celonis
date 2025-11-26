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

# Try to get or create a dummy job to inspect
try:
    job = data_pool.create_job("INSPECT_JOB")
except Exception:
    jobs = data_pool.get_jobs()
    if jobs:
        job = jobs[0]
    else:
        print("No jobs found and creation failed.")
        exit()

print("Pool methods:", [m for m in dir(data_pool) if 'create' in m or 'get' in m])
print("Job methods:", [m for m in dir(job) if 'create' in m or 'get' in m or 'run' in m or 'execute' in m])


# Clean up if created
if job.name == "INSPECT_JOB":
    job.delete()
