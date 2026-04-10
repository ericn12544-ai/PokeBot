import pandas as pd
import requests
import xml.etree.ElementTree as ET

def load_feeds(path="data/feeds.csv"):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        return pd.DataFrame()

def load_feed_history(path="data/feed_history.csv"):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        return pd.DataFrame(columns=["feed_id","item_id"])

def save_feed_history(df, path="data/feed_history.csv"):
    df.to_csv(path, index=False)

def parse_rss(xml_text):
    root = ET.fromstring(xml_text)
    channel = root.find("channel")
    if channel is None:
        return []
    items = []
    for item in channel.findall("item"):
        guid = item.findtext("guid") or item.findtext("link")
        title = item.findtext("title") or ""
        link = item.findtext("link") or ""
        pub = item.findtext("pubDate") or ""
        items.append({"id": guid, "title": title, "link": link, "pubDate": pub})
    return items

def run_feeds(send_func):
    feeds = load_feeds()
    if feeds.empty:
        return 0

    hist = load_feed_history()
    sent = 0

    for _, f in feeds.iterrows():
        if not bool(f.get("active", True)):
            continue

        r = requests.get(str(f["feed_url"]), timeout=15)
        r.raise_for_status()

        items = parse_rss(r.text)
        keywords = str(f.get("keywords","")).split("|") if f.get("keywords") else []

        for it in items[:25]:
            if ((hist["feed_id"] == f["feed_id"]) & (hist["item_id"] == it["id"])).any():
                continue

            # keyword filter (optional)
            if keywords and not any(k.lower() in it["title"].lower() for k in keywords if k):
                hist.loc[len(hist)] = [f["feed_id"], it["id"]]
                continue

            title = f"📰 TCG News: {it['title']}"
            msg = f"{it['link']}\n{it.get('pubDate','')}"
            send_func(title, msg)

            hist.loc[len(hist)] = [f["feed_id"], it["id"]]
            sent += 1

    save_feed_history(hist)
    return sent