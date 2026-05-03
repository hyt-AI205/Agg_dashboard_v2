from pymongo import MongoClient
col = MongoClient("mongodb://localhost:27017/")["social_scraper"]["scrape_targets"]

# Mark all already-approved discovery profiles as reviewed
result = col.update_many(
    {
        "added_by": "discovery",
        "active": True,                          # already approved
        "discovery_reviewed": {"$exists": False} # but field missing
    },
    {"$set": {"discovery_reviewed": True}}
)
print(f"Fixed {result.modified_count} documents")