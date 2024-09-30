import json
import pandas as pd

# Function to extract the language and code_to_comment_ratio from the UAST
def extract_language_and_ratio(uast_str):
    try:
        uast_json = json.loads(uast_str)
        root_node = uast_json["nodes"]["0"]  # Assuming "0" is always the root node
        language = root_node["metadata"].get("language", None)
        ratio = root_node["metadata"].get("code_to_comment_ratio", None)
        return language, ratio
    except (json.JSONDecodeError, KeyError, TypeError):
        return None, None

# Load your dataset (update the path as needed)
df = pd.read_parquet('multi-package_uast.parquet', 'pyarrow')

# Apply the function to extract language and code_to_comment_ratio
df[['language_from_uast', 'code_to_comment_ratio']] = df['UAST'].apply(lambda uast: pd.Series(extract_language_and_ratio(uast)))

# Group by language and calculate the average code_to_comment_ratio
language_avg_ratio = df.groupby('language_from_uast')['code_to_comment_ratio'].mean().reset_index()

output_file = 'average_code_to_comment_ratio_per_language.csv'
language_avg_ratio.to_csv(output_file, index=False)

print(language_avg_ratio)