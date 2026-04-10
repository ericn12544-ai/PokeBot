# agent/config.py
ENABLE_FEEDS = True
ENABLE_SCHEDULED = True
ENABLE_HEARTBEAT = True
HEARTBEAT_HOURS = 24
# Feed product filters (required in title/summary for alerts)
FEED_REQUIRED_KEYWORDS = [
	"Elite Trainer Box",
	"ETB",
	"Pokemon Center ETB",
	"Super Premium Collection",
	"Premium Collection",
	"Booster Bundle",
	"Booster Box",
	"Collection Box",
	"Build and Battle Box",
	"Mini Tin",
	"Tin",
	"SPC",
	"UPC",
]
# ZIPs you care about
LOCAL_ZIPS = {"60491", "60441", "60451", "60462"}

# Alert threshold
ALERT_SCORE_THRESHOLD = 3

# Discord
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1486944657719169215/Cicg4EW7BKInSO0ZhUwHVeoEyCZVTbTF7bKqJgHMcvy3LTe-Ye5fe6heyb5mq7caj96z"

# File paths
PRODUCTS_CSV = "data/products.csv"
DROP_EVENTS_CSV = "data/drop_events.csv"
DROP_PRODUCTS_CSV = "data/drop_products.csv"
STORE_PATTERNS_CSV = "data/store_patterns.csv"
SCORED_DROPS_CSV = "data/scored_drops.csv"
ALERT_HISTORY_CSV = "data/alert_history.csv"
HEARTBEAT_HISTORY_CSV = "data/heartbeat_history.csv"