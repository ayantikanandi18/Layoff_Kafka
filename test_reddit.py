import praw
import os
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

# ✅ Initialize Reddit API
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)

# ✅ Test Authentication
try:
    print("✅ Authenticated as:", reddit.user.me())
except Exception as e:
    print("❌ Authentication failed:", e)
