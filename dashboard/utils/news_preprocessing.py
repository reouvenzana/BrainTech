import polars as pl
from typing import List, Dict

def load_news_data(file_path: str = "./Data/News/news_with_topics.parquet") -> pl.DataFrame:
    """
    Load news data from a Parquet file.

    Args:
        file_path (str): The path to the Parquet file.

    Returns:
        pl.DataFrame: The loaded news data.
    """
    return pl.read_parquet(file_path)

def filter_news_by_company(df: pl.DataFrame, company: str) -> pl.DataFrame:
    """
    Filter news data for a specific company.

    Args:
        df (pl.DataFrame): The news data.
        company (str): The company to filter by.

    Returns:
        pl.DataFrame: The filtered news data.
    """
    return df.filter(pl.col("companies") == company)

def get_topic_dict(df: pl.DataFrame) -> List[Dict[str, str]]:
    """
    Get a list of modeled topics from the news about a specific company.

    Args:
        df (pl.DataFrame): The news data.

    Returns:
        List[Dict[str, str]]: A list of dictionaries with topic labels and values.
    """
    df_topics = (
        df
        .select(["topics", "topics_custom_name", "topics_count"])
        .explode(["topics", "topics_custom_name", "topics_count"])
        .unique(["topics", "topics_custom_name", "topics_count"])
        .drop_nulls()
        .sort("topics_count", descending=True)
        .with_columns(pl.col("topics").cast(pl.Int8).cast(pl.Utf8))
    )
    return [{"label": row[1], "value": row[0]} for row in df_topics.rows()]

def get_company_dict(df: pl.DataFrame) -> List[Dict[str, str]]:
    """
    Get a list of unique companies.

    Args:
        df (pl.DataFrame): The unfiltered news data.

    Returns:
        List[Dict[str, str]]: A list of dictionaries with company labels and values.
    """
    unique_companies = df.select("companies").unique().sort("companies")
    return [{"label": row[0], "value": row[0]} for row in unique_companies.rows()]

def get_news_elements(df: pl.DataFrame, index: int) -> str:
    """
    Get the text of a news article at a specific index, with newlines formatted.

    Args:
        df (pl.DataFrame): The news data.
        index (int): The index of the news article.

    Returns:
        str: The formatted text of the news article.
    """
    news = (
        df
        .with_columns(pl.col("text").str.replace_all(r"\n", r"\n\n"))
        [index]
    )

    news_title = news[0, "title"]
    news_date_published = news[0,"date_published"]
    news_text = news[0, "text"].replace(r"\n", "\n")
    news_link = news[0, "link"]

    return news_title, news_date_published, news_text, news_link

