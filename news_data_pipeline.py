import polars as pl
from datetime import datetime

def extract_transform_load():
    try:
        # Define input and output paths
        input_path = "./Data/News/News.csv"
        output_path = "./Data/News/news_cleaned.parquet"

        # Read CSV file
        df = pl.read_csv(input_path)

        # Define company names of interest
        companies = ["Berkshire Hathaway", "JPMorgan", "Bank of America",
                     "Wells Fargo", "CVS Health", "UnitedHealth", "McKesson",
                     "AmerisourceBergen", "Walmart", "Costco",
                     "Kroger", "Home Depot", "General Motors",
                     "Boeing", "Caterpillar", "Ford"]

        pattern_companies = r"\b" + "|".join(companies) + r"\b"

        # Transform and clean the data
        df_cleaned = (
            df
            .select(["Titre1", "Lien_du_titre", "texte1", "Date de publication"])
            .filter(pl.col("Date de publication").is_not_null())
            .with_columns(
                pl.col("Date de publication")
                .str.extract(r"\b([A-Za-z]+\s\d{1,2},\s\d{4})\b", 1)
                .str.strptime(pl.Date, "%B %d, %Y"),
                companies=pl.col("texte1").str.extract_all(pattern_companies).list.unique(),
                texte1=(
                    pl.col("texte1")
                    .str.replace_all(r"\t", "")
                    .str.replace_all(r"(\s*\n)+", "\n")
                    .str.replace_all(r"\s{2,}.*\s", "")
                    .str.replace_all(r"Image Credits.*\s", "\n")
                    .str.replace_all(r"[^.?!]*[^.?!\s][^.?!]*$", "")
                )
            )
            .filter(pl.col("companies").list.len() > 0)
            .rename(
                {"Titre1": "title",
                 "Lien_du_titre": "link",
                 "texte1": "text",
                 "Date de publication": "date_published"}
            )
        )

        # Write cleaned data to Parquet file
        df_cleaned.write_parquet(output_path)

        print(f"Data pipeline completed successfully at {datetime.now()}.")
    except Exception as e:
        print(f"Error occurred during data pipeline execution: {str(e)}")

if __name__ == "__main__":
    extract_transform_load()
