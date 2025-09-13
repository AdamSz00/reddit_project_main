# import os
import pandas as pd
import torch
from transformers import BertTokenizer, BertForSequenceClassification
from torch.utils.data import DataLoader, TensorDataset

# Model directory
MODEL_DIR = "/Users/adam/Documents/Python/unused_reddit/Saved_model"

# load device
device = torch.device(
    "cuda"
    if torch.cuda.is_available()
    else "mps"
    if torch.backends.mps.is_available()
    else "cpu"
)

# load BERT tokenizer and model
BERT_TOKENIZER = BertTokenizer.from_pretrained(MODEL_DIR)
BERT_MODEL = BertForSequenceClassification.from_pretrained(MODEL_DIR, num_labels=6).to(
    device
)


def classify_toxicity_multilabel(
    texts, tokenizer=BERT_TOKENIZER, model=BERT_MODEL, device=device, batch_size=32
):
    """Function to classify toxicity of texts using a pre-trained BERT model.
    Args:
        texts (list): List of text strings to classify.
        tokenizer (BertTokenizer): Tokenizer for BERT model.
        model (BertForSequenceClassification): Pre-trained BERT model.
        device (torch.device): Device to run the model on.
        batch_size (int): Batch size for processing texts.
    Returns:
        list: List of toxicity scores for each text.
    """
    model.eval()
    encodings = tokenizer(
        texts, truncation=True, padding=True, return_tensors="pt", max_length=512
    )
    dataset = TensorDataset(encodings["input_ids"], encodings["attention_mask"])
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=False)

    all_predictions = []

    for batch in dataloader:
        input_ids, attention_mask = [t.to(device) for t in batch]
        with torch.no_grad():
            outputs = model(input_ids, attention_mask=attention_mask)
            logits = outputs.logits
            predictions = torch.sigmoid(logits).cpu().numpy()
            all_predictions.extend(predictions)

    return all_predictions


def run_toxicity_analysis(df, chunk_size=128):
    """Function to run toxicity analysis on a DataFrame of Reddit posts.
    Args:
        df (pd.DataFrame): DataFrame containing Reddit posts with 'post_id' and 'body' columns.
        chunk_size (int): Number of posts to process in each batch.
    Returns:
        pd.DataFrame: DataFrame with toxicity scores added.
    """
    # Check if the input DataFrame is empty
    if df.empty:
        return pd.DataFrame()

    # Create a new DataFrame to store the results
    results_df = df[["post_id", "body"]].copy()

    # Initialize columns for toxicity scores
    for col in [
        "toxic",
        "severe_toxic",
        "obscene",
        "threat",
        "insult",
        "identity_hate",
        "overall_toxicity",
    ]:
        results_df[col] = None

    # Process posts in chunks
    for start in range(0, len(results_df), chunk_size):
        end = start + chunk_size
        df_chunk = results_df.iloc[start:end]
        texts = df_chunk["body"].fillna("").tolist()

        # returns a list of dictionaries with toxicity scores
        toxicity_scores = classify_toxicity_multilabel(texts)

        # Create a DataFrame from the toxicity scores
        toxicity_df = pd.DataFrame(
            toxicity_scores,
            columns=[
                "toxic",
                "severe_toxic",
                "obscene",
                "threat",
                "insult",
                "identity_hate",
            ],
        )
        toxicity_df["overall_toxicity"] = toxicity_df.max(axis=1)

        # Update the results_df with the new scores
        results_df.loc[df_chunk.index, toxicity_df.columns] = toxicity_df.values

    return results_df


if __name__ == "__main__":
    # Example usage
    df = pd.read_csv(
        "/Users/adam/Documents/Python/reddit_project_main/PT/UT/processed_data.csv"
    )
    df = df[:500]

    results = run_toxicity_analysis(df, chunk_size=128)
    print(results.columns)
    print(results)
