import logging as lg
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy import text


def connect_to_database():
    """Generates a SQLAlchemy ENGINE instance to connect to the PostgreSQL database.

    Returns:
        engine_instance (object): SQLAlchemy engine instance connected to the PostgreSQL database.
    """

    username = "postgres"
    password = "1001"
    database = "reddit_database"
    host = "localhost"
    port = "5432"

    db_engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    )

    try:
        with db_engine.connect() as connection:
            result = connection.execute(text("SELECT 1;"))
            lg.info("Connection successful: %s ", result.scalar() == 1)
    except Exception as e:
        lg.error("Connection failed: %s", e)
    finally:
        db_engine.dispose()
        lg.info("Connection closed.")

    return db_engine


def get_existing_post_ids(engine_instance, table):
    """
    Fetches existing post IDs from a table (ex.: 'reddit_posts')  in the database.

    Args:
        engine_instance: A SQLAlchemy engine instance connected to the target database.
        table: a table we need IDs

    Returns:
        set: A set of post IDs (strings or integers) retrieved from the database.
             Returns an empty set if the query fails.
    """

    try:
        known_id = pd.read_sql(f"SELECT post_id FROM {table}", engine_instance)
        return set(known_id["post_id"].tolist())
    except Exception as e:
        lg.error("Could not fetch post_ids from DB: %s", e)
        return set()


def get_existing_username_mapping(engine_instance):
    """Get complete username mapping (author -> new_username) from database
    Args:
        engine_instance: A SQLAlchemy engine instance connected to the target database.
    Returns:
        pd.DataFrame: DataFrame with 'original_author' and 'new_username' columns.
                      Returns an empty DataFrame if the query fails.
    """
    try:
        known_users = pd.read_sql(
            "SELECT original_author, new_username FROM unique_authors", engine_instance
        )
        return known_users
    except Exception as e:
        lg.error("Could not fetch username mapping from DB: %s", e)
        return known_users.DataFrame(columns=["original_author", "new_username"])


##########################################


def insert_data(data, engine_instance, table_name):
    """
    Inserts data into the specified table.
    Assumes duplicates have been filtered before calling this function.
    """
    try:
        df = data.copy()
        if df.empty:
            lg.info(f"No new records to insert into {table_name}.")
            return

        df.to_sql(
            table_name, engine_instance, if_exists="append", index=False, chunksize=500
        )
        lg.info(f"✅ {len(df)} records inserted into {table_name}.")
    except Exception as e:
        lg.error(f"Insert into {table_name} failed: {e}")


###########################################


if __name__ == "__main__":
    from logger_config import init_logger

    init_logger()

    lg.info("Running basic DB connection test...")
    engine = connect_to_database()

    lg.info("Fetching known post IDs...")
    ids = get_existing_post_ids(engine, "reddit_posts")
    lg.info("Retrieved %d post IDs", len(ids))

    lg.info("Fetching username mapping...")
    users = get_existing_username_mapping(engine)
    lg.info("Retrieved %d username mappings", len(users))
