import os
import logging as lg
import time
import yaml
import pandas as pd

# My imports
# from logger_config import init_logger
from sql_connect3 import (
    connect_to_database,
    get_existing_post_ids,
    insert_data,
)
from bert_analysis4 import run_toxicity_analysis

########################################################################

# settings from YAML
CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "config", "settings.yaml"
)
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRAPE_LIMIT = config.get("scrape_limit", 1)
TARGET_COMMUNITY = config.get("target_community", "gaming")
LOG_LEVEL = config.get("log_level", "INFO")


CREDENTIALS = "/Users/adam/Documents/reddit_credencials/reddit_credentials.txt"

########################################################################

# knw_ids - list of post_ids that are already in the database
# new_rows  - DataFrame with new posts and hide usernames that are not in the database
# knw_user_map - OLD DataFrame with known authors and their new usernames
# local_mapping_df - NEW DataFrame with original and hide usernames for new authors


# 1️⃣ CONNECT TO DB
def connect_db():
    start = time.time()
    lg.info("TESTING CONNECTION TO DATABASE...")
    engine = connect_to_database()
    lg.info("CONNECTION TEST SUCCESSFUL (%.2fs).", time.time() - start)
    return engine


def get_selected_ids(set_of_ids, engine_instance, table):
    # Sprawdzamy, czy lista ID nie jest pusta, aby uniknąć błędu w zapytaniu.
    if not set_of_ids:
        return pd.DataFrame()

    query = f"SELECT post_id, body FROM {table} WHERE post_id IN %(ids)s"

    df = pd.read_sql(query, engine_instance, params={"ids": tuple(set_of_ids)})

    return df


# 6️⃣ INSERT NEW AUTHORS
def insert_authors(local_mapping_df, engine):
    lg.info("INSERTING INTO: 'Unique_authors'...")

    # insert new authors to the database
    if not local_mapping_df.empty:
        local_mapping_df = local_mapping_df.rename(
            columns={"author": "original_author"}
        )
        insert_data(
            data=local_mapping_df,  # DataFrame with new authors
            engine_instance=engine,  # SQLAlchemy engine
            table_name="unique_authors",  # Target table name
        )
    else:
        lg.info("No new authors to insert.")


# 7️⃣ INSERT NEW POSTS
def insert_posts(new_rows, engine):
    lg.info("INSERTING INTO: 'reddit_posts'...")
    # drop original_author column if exists
    if "original_author" in new_rows.columns:
        new_rows = new_rows.drop(columns=["original_author"])
    insert_data(
        data=new_rows,
        engine_instance=engine,
        table_name="reddit_posts",
    )
    lg.info("Inserted %d new posts", len(new_rows))


# 8️⃣ TOXICITY ANALYSIS & INSERT
def analyze_toxicity(new_rows):
    # new ids
    if new_rows.empty:
        lg.info("No new posts to analyze for toxicity.")
        return
    lg.info("RUNNING TOXICITY ANALYSIS ON NEW RECORDS...")
    df_new_ids = new_rows[["post_id", "body"]].copy()
    lg.info("Analyzing %d new posts for toxicity...", len(df_new_ids))

    # run bert
    results = run_toxicity_analysis(df_new_ids)
    lg.info("TOXICITY ANALYSIS COMPLETE.")
    return results


def insert_toxicity(results, engine):
    # insert data to the database
    if "body" in results.columns:
        results = results.drop(columns=["body"])

    lg.info("INSERTING INTO: 'toxicity_results")
    insert_data(
        data=results,
        engine_instance=engine,
        table_name="toxicity_results",
    )
    lg.info("Inserted %d new toxicity records", len(results))


def main():
    engine = connect_db()
    knw_ids_toxicity = get_existing_post_ids(engine, "toxicity_results")
    knw_ids_posts = get_existing_post_ids(engine, "reddit_posts")
    new_for_toxicity = knw_ids_posts - knw_ids_toxicity

    print(len(new_for_toxicity))

    new_rows = get_selected_ids(new_for_toxicity, engine, "reddit_posts")

    results = analyze_toxicity(new_rows[:10000])

    insert_toxicity(results, engine)


##########################################################
##########################################################
if __name__ == "__main__":
    main()
