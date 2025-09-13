import os
import logging as lg
import time
import yaml
import pandas as pd

# My imports
from UT.logger_config import init_logger
from UT.reddit_scrapper1 import get_reddit_cr, collect_reddit_data
from UT.transform2 import transform_reddit_data, hide_usernames
from UT.sql_connect3 import (
    connect_to_database,
    get_existing_post_ids,
    get_existing_username_mapping,
    insert_data,
)
from UT.bert_analysis4 import run_toxicity_analysis

########################################################################

# settings from YAML
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "settings.yaml")
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


# 2️⃣ SCRAPING DATA
def scrape_data():
    start = time.time()
    lg.info("SCRAPING DATA START...")
    reddit_instance = get_reddit_cr(CREDENTIALS)
    all_data = collect_reddit_data(
        reddit_instance, TARGET_COMMUNITY, limit=SCRAPE_LIMIT
    )
    # roosterteeth, BendyAndTheInkMachine
    df = pd.DataFrame(all_data)
    lg.info(
        "SCRAPING DATA SUCCESSFUL: %d RECORDS (%.2fs).", len(df), time.time() - start
    )
    return df


# 3️⃣ DATA TRANSFORMATION
def transform_data(df):
    start = time.time()
    lg.info("DATA TRANSFORMATION START...")
    df = transform_reddit_data(df)
    lg.info("DATA TRANSFORMATION SUCCESSFUL (%.2fs)...", time.time() - start)
    return df


# 4️⃣ FILTER NEW ROWS
def filter_new_rows(df, engine):
    start = time.time()
    lg.info("FILTERING NEW ROWS...")
    knw_ids = get_existing_post_ids(engine, "reddit_posts")
    lg.info("length of known post_id: %d", len(knw_ids))

    # FILTER NEW ROWS
    new_rows = df[~df["post_id"].isin(knw_ids)].copy()
    if new_rows.empty:
        lg.info("NO NEW ROWS TO PROCESS. EXITING (%.2fs)...", time.time() - start)
        return None, knw_ids
    lg.info("FOUND (%d) NEW ROWS (%.2fs)", len(new_rows), time.time() - start)
    return new_rows, knw_ids


# 5️⃣ ANONYMIZE USERNAMES
def anonymize_usernames(new_rows, engine):
    lg.info("ANONYMIZING USERNAMES...")
    knw_user_map = get_existing_username_mapping(engine)
    new_rows, local_mapping_df = hide_usernames(new_rows, knw_user_map)
    lg.info("HIDING ORIGINAL USERNAMES SUCCESSFULL.")

    return new_rows, local_mapping_df


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


# 9️⃣ INSERT TOXICITY RESULTS
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


def main():
    init_logger()
    lg.info("=== START OF ETL PROCESS ===")
    start_time = time.time()

    try:
        engine = connect_db()
        raw_df = scrape_data()
        transformed_df = transform_data(raw_df)

        new_rows, knw_ids = filter_new_rows(transformed_df, engine)
        if new_rows is None:
            return

        new_rows, local_mapping_df = anonymize_usernames(new_rows, engine)
        insert_authors(local_mapping_df, engine)
        insert_posts(new_rows, engine)
        results = analyze_toxicity(new_rows)
        insert_toxicity(results, engine)

    except Exception as e:
        lg.error(f"AN ERROR OCCURRED: {e}")
    finally:
        lg.info("=== END OF ETL PROCESS (%.2fs) ===", time.time() - start_time)


##########################################################
##########################################################
if __name__ == "__main__":
    main()
