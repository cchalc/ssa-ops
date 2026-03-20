# Databricks notebook source
# MAGIC %md
# MAGIC # ASQ Overdue Chatter Bot
# MAGIC
# MAGIC This notebook finds overdue SSA ASQs for your direct reports and posts Chatter comments prompting for updates.
# MAGIC
# MAGIC **Authentication:** Uses OAuth access token from `sf` CLI stored in Databricks secrets.
# MAGIC
# MAGIC **⚠️ Token Refresh:** If you get auth errors, run this locally to refresh:
# MAGIC ```bash
# MAGIC # Refresh token and update secrets
# MAGIC sf org login web --alias christopher.chalcraft@databricks.com
# MAGIC TOKEN=$(sf org display --target-org christopher.chalcraft@databricks.com --json | python3 -c "import sys, json; print(json.load(sys.stdin)['result']['accessToken'])")
# MAGIC databricks secrets put-secret salesforce access_token --string-value "$TOKEN" --profile DEFAULT
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install simple-salesforce
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from simple_salesforce import Salesforce
from datetime import date, datetime
from typing import List, Dict
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Direct reports - Salesforce User IDs
DIRECT_REPORT_IDS = [
    "005Vp000002lC2zIAE",  # Volodymyr Vragov
    "0058Y00000CPeiKQAT",  # Allan Cao
    "0058Y00000CP6yKQAT",  # Harsha Pasala
    "0053f000000Wi00AAC",  # Réda Khouani
    "005Vp0000016p45IAA",  # Scott McKean
    "0058Y00000CPn0bQAD",  # Mathieu Pelletier
]

# Chatter comment to post on overdue ASQs
OVERDUE_COMMENT = "Overdue. Please update target end date, close, or add some request status notes or commentary to explain."

# Set to False for dry-run (just show what would be posted)
POST_COMMENTS = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Salesforce

# COMMAND ----------

def connect_to_salesforce() -> Salesforce:
    """Connect to Salesforce using OAuth access token from secrets."""
    try:
        access_token = dbutils.secrets.get(scope="salesforce", key="access_token")
        instance_url = dbutils.secrets.get(scope="salesforce", key="instance_url")

        sf = Salesforce(
            instance_url=instance_url,
            session_id=access_token
        )

        # Test the connection
        sf.query("SELECT Id FROM User LIMIT 1")
        print("✓ Connected to Salesforce")
        return sf

    except Exception as e:
        error_msg = str(e)
        if "INVALID_SESSION_ID" in error_msg or "Session expired" in error_msg:
            print("✗ Token expired! Run this locally to refresh:")
            print("")
            print("  sf org login web --alias christopher.chalcraft@databricks.com")
            print('  TOKEN=$(sf org display --target-org christopher.chalcraft@databricks.com --json | python3 -c "import sys, json; print(json.load(sys.stdin)[\'result\'][\'accessToken\'])")')
            print('  databricks secrets put-secret salesforce access_token --string-value "$TOKEN" --profile DEFAULT')
            print("")
        raise Exception(f"Failed to connect to Salesforce: {e}")

sf = connect_to_salesforce()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Overdue ASQs

# COMMAND ----------

def get_overdue_asqs(sf: Salesforce, owner_ids: List[str]) -> List[Dict]:
    """Query Salesforce for overdue SSA ASQs owned by specified users."""

    today = date.today().isoformat()
    owner_list = "','".join(owner_ids)

    query = f"""
        SELECT
            Id,
            Name,
            Account_Name__c,
            Account__c,
            Owner.Name,
            OwnerId,
            Status__c,
            Support_Type__c,
            AssignmentDate__c,
            End_Date__c,
            Request_Description__c,
            Request_Status_Notes__c
        FROM ApprovalRequest__c
        WHERE OwnerId IN ('{owner_list}')
            AND Status__c IN ('In Progress', 'Under Review', 'On Hold')
            AND Request_Type__c = 'Specialist SA (SSA) Request'
            AND End_Date__c < {today}
        ORDER BY End_Date__c ASC
    """

    result = sf.query_all(query)
    records = result.get('records', [])

    # Calculate days overdue for each record
    for record in records:
        if record.get('End_Date__c'):
            end_date = datetime.strptime(record['End_Date__c'], '%Y-%m-%d').date()
            record['days_overdue'] = (date.today() - end_date).days
        else:
            record['days_overdue'] = 0

    return records

overdue_asqs = get_overdue_asqs(sf, DIRECT_REPORT_IDS)
print(f"Found {len(overdue_asqs)} overdue ASQs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Overdue ASQs

# COMMAND ----------

# Create a summary DataFrame
if overdue_asqs:
    summary_data = []
    for asq in overdue_asqs:
        summary_data.append({
            "AR": asq.get('Name'),
            "Account": asq.get('Account_Name__c'),
            "Owner": asq.get('Owner', {}).get('Name') if asq.get('Owner') else 'Unknown',
            "End Date": asq.get('End_Date__c'),
            "Days Overdue": asq.get('days_overdue'),
            "Status": asq.get('Status__c'),
            "Support Type": asq.get('Support_Type__c'),
            "SFDC Link": f"https://databricks.lightning.force.com/lightning/r/ApprovalRequest__c/{asq.get('Id')}/view"
        })

    df = spark.createDataFrame(summary_data)
    display(df.orderBy("Days Overdue", ascending=False))
else:
    print("🎉 No overdue ASQs found! Your team is on track.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Chatter Comments

# COMMAND ----------

def post_chatter_comment(sf: Salesforce, parent_id: str, comment: str) -> bool:
    """Post a Chatter comment (FeedItem) on a Salesforce record."""
    try:
        sf.FeedItem.create({
            'ParentId': parent_id,
            'Body': comment
        })
        return True
    except Exception as e:
        print(f"  Error posting to {parent_id}: {e}")
        return False

def post_overdue_comments(sf: Salesforce, asqs: List[Dict], comment: str, dry_run: bool = False) -> Dict:
    """Post Chatter comments on all overdue ASQs."""
    results = {"success": 0, "failed": 0, "skipped": 0}

    for asq in asqs:
        ar_name = asq.get('Name')
        asq_id = asq.get('Id')
        account = asq.get('Account_Name__c')
        owner = asq.get('Owner', {}).get('Name') if asq.get('Owner') else 'Unknown'
        days = asq.get('days_overdue', 0)

        if dry_run:
            print(f"[DRY RUN] Would post to {ar_name} ({account}) - {owner} - {days} days overdue")
            results["skipped"] += 1
        else:
            print(f"Posting to {ar_name} ({account}) - {owner} - {days} days overdue... ", end="")
            if post_chatter_comment(sf, asq_id, comment):
                print("✓")
                results["success"] += 1
            else:
                print("✗")
                results["failed"] += 1

    return results

# COMMAND ----------

# Post comments (or dry-run)
if overdue_asqs:
    print(f"\n{'=' * 60}")
    print(f"{'DRY RUN - ' if not POST_COMMENTS else ''}Posting Chatter comments on {len(overdue_asqs)} overdue ASQs")
    print(f"{'=' * 60}\n")

    results = post_overdue_comments(sf, overdue_asqs, OVERDUE_COMMENT, dry_run=not POST_COMMENTS)

    print(f"\n{'=' * 60}")
    print(f"Results: {results['success']} posted, {results['failed']} failed, {results['skipped']} skipped (dry-run)")
    print(f"{'=' * 60}")
else:
    print("No overdue ASQs to process.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by Owner

# COMMAND ----------

if overdue_asqs:
    from collections import Counter

    owner_counts = Counter(
        asq.get('Owner', {}).get('Name') if asq.get('Owner') else 'Unknown'
        for asq in overdue_asqs
    )

    print("\nOverdue ASQs by Owner:")
    print("-" * 40)
    for owner, count in owner_counts.most_common():
        print(f"  {owner}: {count}")
