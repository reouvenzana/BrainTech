from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired, OpenAI
from bertopic.vectorizers import ClassTfidfTransformer
from sentence_transformers import SentenceTransformer
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer
import openai
import tiktoken
import polars as pl
import os
from typing import List, Tuple, Optional

def extract_company_news(df: pl.DataFrame, company: str) -> List[str]:
    """
    Extracts news articles related to a specific company from the dataframe.

    Args:
        df (pl.DataFrame): The dataframe containing news articles.
        company (str): The company name to filter news articles.

    Returns:
        List[str]: A list of news articles related to the specified company.
    """
    return (
        df.filter(pl.col("companies").list.contains(company))
          .select(pl.col("text"))
          .to_series()
          .to_list()
    )

def extract_topics(documents: List[str]) -> Tuple[Optional[BERTopic], Optional[pl.DataFrame]]:
    """
    Extracts topics from a list of documents using BERTopic.

    Args:
        documents (List[str]): The list of documents to analyze.

    Returns:
        Tuple[Optional[BERTopic], Optional[pl.DataFrame]]: The topic model and topic distribution dataframe.
    """
    # Use OpenAI API to generate more human-friendly topic names
    client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    prompt = """
    I have a topic that contains the following documents: 
    [DOCUMENTS]
    The topic is described by the following keywords: [KEYWORDS]

    Based on the information above, extract a short topic label in the following format:
    topic: <topic label>
    """

    tokenizer = tiktoken.encoding_for_model("gpt-3.5-turbo")

    aspect_model = OpenAI(
    client,
    model="gpt-3.5-turbo",
    delay_in_seconds=10, 
    chat=True, 
    prompt=prompt,
    diversity=0.1, 
    doc_length=500,
    tokenizer=tokenizer)

    # KeyBERTInspired offers better topics extraction
    main_representation = KeyBERTInspired()
    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    hdbscan_model = HDBSCAN(min_cluster_size=3, metric='euclidean', cluster_selection_method='eom', prediction_data=True)
    vectorizer_model = CountVectorizer(stop_words="english", ngram_range=(1, 2))

    representation_model = {
        "Main": main_representation,
        "Aspect1": aspect_model
    }

    topic_model = BERTopic(
        embedding_model=embedding_model,
        representation_model=representation_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer_model,
        nr_topics=20
    )

    # The corpus may be too small for BERTopic to extract topics, which can cause an error
    try:
        topics, probs = topic_model.fit_transform(documents)
        topic_distr = topic_model.approximate_distribution(documents)
    except Exception as e:
        return None, None

    # Add custom labels generated with OpenAI to the model
    topic_labels = topic_model.generate_topic_labels(topic_prefix=False, aspect="Aspect1")
    topic_model.set_topic_labels(topic_labels)

    return topic_model, topic_distr

def add_topics_to_df(df: pl.DataFrame, company: str, topic_model: BERTopic, topic_distr: pl.DataFrame) -> pl.DataFrame:
    """
    Adds topic information to the dataframe for a specific company.

    Args:
        df (pl.DataFrame): The original dataframe.
        company (str): The company name.
        topic_model (BERTopic): The topic model.
        topic_distr (pl.DataFrame): The topic distribution dataframe.

    Returns:
        pl.DataFrame: The dataframe with added topic information.
    """
    return (
        df.filter(pl.col("companies").list.contains(company))
          .with_row_index(name="index", offset=0)
          .with_columns(pl.lit(company).alias("companies"))
          .join(
              pl.DataFrame(topic_distr)
                .with_row_index(name="index", offset=0)
                .melt(id_vars="index", value_vars=pl.col("*"))
                .sort("index")
                .filter(pl.col("value") > 0.20)
                .with_columns(pl.col("variable").str.extract(r"(\d)").cast(pl.Int64).alias("topics"))
                .drop("variable"),
              on="index",
              how="left",
              validate='1:m',
              coalesce=True
          )
          .join(
              pl.DataFrame(topic_model.get_topic_info())
                .select(pl.col(["Topic", "Count", "Representation", "CustomName"]).alias("topics_")),
              how="left",
              left_on="topics",
              right_on="topics_Topic",
              coalesce=True
          )
          .group_by(["index", "title", "link", "text", "date_published", "companies"], maintain_order=True)
          .all()
          .drop("index")
          .rename({
              "value": "topic_probability_distribution",
              "topics_CustomName": "topics_custom_name"
          })
    )

def save_topic_model(topic_model: BERTopic, company: str) -> None:
    """
    Saves the topic model to a specified path.

    Args:
        topic_model (BERTopic): The topic model to save.
        company (str): The company name to use in the file path.
    """
    path = f"./Models/BERTopic_Models/{company}"
    topic_model.save(path, serialization="safetensors", save_ctfidf=True, save_embedding_model="sentence-transformers/all-MiniLM-L6-v2")

def main():
    path = "./Data/News/news_cleaned.parquet"
    companies = ["Berkshire Hathaway", "JPMorgan", "Bank of America", "Wells Fargo", "CVS Health", "UnitedHealth", "McKesson", "AmerisourceBergen", "Walmart", "Costco", "Kroger", "Home Depot", "General Motors", "Boeing", "Caterpillar", "Ford"]

    df_news = pl.read_parquet(path)

    schema = {
        'title': pl.Utf8,
        'link': pl.Utf8,
        'text': pl.Utf8,
        'date_published': pl.Date,
        'companies': pl.Utf8,
        'topic_probability_distribution': pl.List(pl.Float64),
        'topics': pl.List(pl.Int64),
        'topics_count': pl.List(pl.Int64),
        'topics_representation': pl.List(pl.List(pl.Utf8)),
        'topics_custom_name': pl.List(pl.Utf8)
    }
    global_df_with_topics = pl.DataFrame(schema=schema)

    for company in companies:
        news = extract_company_news(df_news, company)
        print(company)
        topic_model, topic_distr = extract_topics(news)
        if topic_model is None:
            print(f"Error while processing {company}. The number of news may be too small ({len(news)}).")
            continue
        partial_df_with_topics = add_topics_to_df(df_news, company, topic_model, topic_distr)
        global_df_with_topics = pl.concat([global_df_with_topics, partial_df_with_topics], how="vertical")
        save_topic_model(topic_model, company)

    global_df_with_topics.write_parquet("./Data/News/news_with_topics.parquet")

if __name__ == "__main__":
    main()