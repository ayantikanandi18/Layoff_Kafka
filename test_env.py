import os
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

# ✅ Print values (for testing)
print("Client ID:", os.getenv("REDDIT_CLIENT_ID"))
print("Client Secret:", os.getenv("REDDIT_CLIENT_SECRET"))
print("User Agent:", os.getenv("REDDIT_USER_AGENT"))
