import string as st
import logging
import pandas as pd


# 1
def clean_raw_data(df):
    return df.dropna(subset=["author"]).copy()


# 2
def rename_columns(df):
    column_names = {
        "id": "post_id",
        "parent_id": "target_post_id",
        "permalink": "id_url",
    }
    return df.rename(columns=column_names)


# 3
def create_community_column(df):
    df["community"] = df.apply(
        lambda row: row["id_url"].split("/")[2] if pd.notnull(row["id_url"]) else None,
        axis=1,
    )
    return df


# 4
def create_date_time_columns(df):
    df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s")
    df["date"] = df.created_utc.dt.date
    df["time"] = df.created_utc.dt.time
    df.sort_values(by="created_utc", inplace=True, ascending=False)
    df.drop(columns=["created_utc"], inplace=True)
    return df


# 5
def map_target_post_id_to_author(df):
    id_to_author = dict(zip(df["post_id"], df["author"]))
    df["target_author"] = df["target_post_id"].map(id_to_author)

    return df


# 6
def get_nr_of_replies(df):
    reply_cn = df["target_post_id"].value_counts().reset_index()
    reply_cn.columns = ["target_post_id", "replies_cn"]
    reply_cn = dict(zip(reply_cn["target_post_id"], reply_cn["replies_cn"]))
    df["number_of_replies"] = df["post_id"].map(reply_cn).fillna(0)

    return df


# 7
def order_columns(df):
    desired_order = [
        "type",
        "submission_id",
        "post_id",
        "target_post_id",
        "author",
        # "original_author",  # Keep original author for reference
        "target_author",
        "community",
        "title",
        "body",
        "score",
        "number_of_replies",
        "date",
        "time",
    ]
    df = df[desired_order]

    return df


# MAIN
def transform_reddit_data(data_input):
    """Transforms raw Reddit data into a structured DataFrame.
    1. Clean transformed data ready for analysis
    2. Username mapping for database storage

    Args:
        data_input (pd.DataFrame or str): DataFrame containing raw Reddit data or file path to CSV.

    Raises:
        ValueError: If input is neither a DataFrame nor a file path.

    Returns:
        tuple:
            - transformed_data (pd.DataFrame): Clean data with anonymized usernames
            - username_mapping (pd.DataFrame): Map of original → anonymized usernames
    """
    # Handle both DataFrame and file path input
    if isinstance(data_input, pd.DataFrame):
        logging.info("Input is DataFrame - using directly")
        df = data_input.copy()
    elif isinstance(data_input, str):
        logging.info("Input is file path - reading from %s", data_input)
        df = pd.read_csv(data_input)
    else:
        raise ValueError("Input must be either DataFrame or file path (str)")

    # 1
    logging.info("Cleaning raw data...")
    df = clean_raw_data(df)

    # 2
    logging.info("Renaming columns...")
    df = rename_columns(df)

    # 3
    logging.info("Creating 'community' column...")
    df = create_community_column(df)

    # 4
    logging.info("Creating 'date' and 'time' column...")
    df = create_date_time_columns(df)

    # 5
    logging.info("Creating 'target_post_id' column...")
    df = map_target_post_id_to_author(df)

    # 6
    logging.info("Creating 'nr of replies' column...")
    df = get_nr_of_replies(df)

    # 7
    logging.info("Ordering collumns...")
    df = order_columns(df)

    return df


######################################################
######################################################


## USERNAME HIDING FUNCTIONS
def _generate_ids_sequential(count, start_num, start_letter):
    """Generate sequential user IDs from a given starting point."""
    user_ids = []
    letters = st.ascii_lowercase
    letter_index = letters.index(start_letter)

    for _ in range(count):
        letter_index += 1
        if letter_index >= len(letters):  # Overflow -> increment number
            letter_index = 0
            start_num += 1
        user_ids.append(f"user{start_num:04d}{letters[letter_index]}")

    return user_ids


def _parse_last_id(last_id):
    """Extract number and letter from last user ID."""
    number = int(last_id[4:8])  # 0003
    letter = last_id[8]  # b
    return number, letter


def hide_usernames(df, existing_mapping_df=None):
    """
    Hides usernames using existing mapping and adds new authors with sequential IDs.

    Args:
        df (pd.DataFrame): DataFrame with 'author' and 'target_author' columns.
        existing_mapping_df (pd.DataFrame): Existing mapping with 'original_author' and 'new_username'.

    Returns:
        tuple: (df with anonymized usernames, new mappings DataFrame)
    """
    # Initialize existing mapping
    if existing_mapping_df is None or existing_mapping_df.empty:
        logging.info("No existing mapping provided - starting fresh")
        existing_mapping_df = pd.DataFrame(columns=["original_author", "new_username"])
        last_id = "user0000a"
    else:
        last_id = existing_mapping_df["new_username"].max()  # e.g., user0003b
        logging.info("Using existing mapping with %d entries", len(existing_mapping_df))

    # Check required columns
    required_columns = ["author", "target_author"]
    if not all(col in df.columns for col in required_columns):
        missing = [col for col in required_columns if col not in df.columns]
        logging.error("Missing required columns: %s", missing)
        raise ValueError(f"DataFrame missing required columns: {missing}")

    df["original_author"] = df["author"]

    # Extract authors and find new ones
    current_authors = set(df["author"].unique())
    known_authors = set(existing_mapping_df["original_author"])
    new_authors = current_authors - known_authors

    logging.info("Found %d new authors", len(new_authors))

    # Generate new usernames if needed
    new_authors_df = pd.DataFrame(columns=["original_author", "new_username"])
    if new_authors:
        start_num, start_letter = _parse_last_id(last_id)
        new_user_ids = _generate_ids_sequential(
            len(new_authors), start_num, start_letter
        )
        new_authors_df = pd.DataFrame(
            {"original_author": list(new_authors), "new_username": new_user_ids}
        )

    # Combine mappings
    combined_mapping = pd.concat(
        [existing_mapping_df, new_authors_df], ignore_index=True
    )

    # Map authors in df
    mapping_dict = dict(
        zip(combined_mapping["original_author"], combined_mapping["new_username"])
    )
    df["author"] = df["author"].map(mapping_dict)
    df["target_author"] = df["target_author"].map(mapping_dict)

    logging.info("Anonymization complete. Total mappings: %d", len(combined_mapping))
    return df, new_authors_df


if __name__ == "__main__":
    from logger_config import init_logger

    init_logger()

    FILE_PATH = "/Users/adam/Documents/Python/reddit_project_main/PT/raw_data.csv"

    # Example usage
    logging.info("Starting data transformation...")
    df = pd.read_csv(FILE_PATH)
    logging.info("Raw data loaded from %s", FILE_PATH)
    logging.info("Transforming data...")
    df = transform_reddit_data(df)
    logging.info("Data transformation completed.")
    print(df.columns)

    df, new_authors_df = hide_usernames(df)

    print(df.head(2))
    print(new_authors_df.head(2))
