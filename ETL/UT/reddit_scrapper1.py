import logging as lg
import time
import pandas as pd
import praw


def get_submission_details(submission, all_data):
    """
    Extracts details from a Reddit submission and appends them to the provided data list.

    Args:
        submission: A Reddit submission object (typically from PRAW) containing post data.
        all_data (list): A list to which the extracted submission details will be appended as a dictionary.

    The extracted details include:
        - type: The type of the item ("submission").
        - submission_id: The unique ID of the submission.
        - id: The unique ID of the submission (same as submission_id).
        - author: The username of the submission's author, or None if deleted.
        - parent_id: Always None for submissions.
        - title: The title of the submission.
        - body: The selftext (body) of the submission.
        - score: The score (upvotes - downvotes) of the submission.
        - created_utc: The UTC timestamp when the submission was created.
        - permalink: The permalink to the submission.
    """
    submission_details = {
        "type": "submission",
        "submission_id": submission.id,
        "id": submission.id,
        "author": submission.author.name if submission.author else None,
        "parent_id": None,
        "title": submission.title,
        "body": submission.selftext,
        "score": submission.score,
        "created_utc": submission.created_utc,
        "permalink": submission.permalink,
    }
    all_data.append(submission_details)


def get_comment_details(comment, submission_id, all_data, parent_id=None):
    """Extracts details from a Reddit comment and appends them to the provided data list.

    Args:
        comment (_type_): Reddit comment object (typically from PRAW) containing comment data.
        submission_id (_type_): ID of the parent submission.
        all_data (_type_): A list to which the extracted comment details will be appended as a dictionary.
        parent_id (_type_, optional): ID of the parent comment if it's a reply; otherwise None. Defaults to None.
    """
    parent_id = (
        parent_id or submission_id
    )  # setting parent_id to submissino_id for top-level comments

    comment_details = {
        "type": "comment",
        "submission_id": submission_id,
        "id": comment.id,
        "author": comment.author.name if comment.author else None,
        "parent_id": parent_id,
        "title": None,
        "body": comment.body,
        "score": comment.score,
        "created_utc": comment.created_utc,
        "permalink": comment.permalink,
    }
    all_data.append(comment_details)

    # Fetch and print replies recursively
    comment.replies.replace_more(limit=0)
    for reply in comment.replies:
        get_comment_details(reply, submission_id, all_data, comment.id)


def get_reddit_cr(credentials_file_path):
    """Reads credentials and returns an authenticated Reddit instance

    Returns:
        reddit_connection (object): Reddit API instance.
    """
    lg.info("Connecting to Reddit...")
    credentials = {}
    with open(credentials_file_path, "r", encoding="utf-8") as file:
        for line in file:
            key, value = line.strip().split("=", 1)
            credentials[key] = value

    reddit = praw.Reddit(
        client_id=credentials["CLIENT_ID"],
        client_secret=credentials["CLIENT_SECRET"],
        user_agent=credentials["USER_AGENT"],
    )

    lg.info("Authenticated as: %s", reddit.user.me())

    return reddit


def collect_reddit_data(reddit_connection, subreddit_name, limit=10):
    """Collects data from a specified subreddit.

    Args:
        reddit_connection (object): Reddit API instance.
        subreddit_name (str): Name of the subreddit to scrape data from.
        limit (int, optional): Maximum number of posts to scrape from the subreddit. Defaults to 10.

    Returns:
        List: Scraped data as a list of dictionaries.
    """

    lg.info("Data collection start...")

    all_data = []

    subreddit_obj = reddit_connection.subreddit(subreddit_name)
    new_submissions = list(subreddit_obj.new(limit=limit))

    try:
        for submission in new_submissions:
            get_submission_details(submission, all_data)

            submission.comments.replace_more(limit=0)
            for comment in submission.comments:
                get_comment_details(comment, submission.id, all_data)

            time.sleep(2)

        lg.info("Data collection end. ")

    except Exception as e:
        lg.error("Error during data collection: %s", e)

    return all_data


if __name__ == "__main__":
    from logger_config import init_logger

    init_logger()

    CR = get_reddit_cr(
        "/Users/adam/Documents/reddit_credencials/reddit_credentials.txt"
    )

    subreddit_data = collect_reddit_data(CR, "roosterteeth", limit=2)

    subreddit_data_df = pd.DataFrame(subreddit_data)
    print(subreddit_data_df.head())
    print("DataFrame created successfully with", len(subreddit_data_df), "records.")
