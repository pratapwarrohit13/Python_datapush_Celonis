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

print(dir(data_pool))
