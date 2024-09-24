import pandas as pd
import os

def merge_csv_files(csv_files,folder):
    merged_data = []
    for csv_file in csv_files:
        df = pd.read_csv(folder+"/"+csv_file)
        merged_data.append(df)
    merged_df = pd.concat(merged_data, ignore_index=True)
    return merged_df



if __name__ == "__main__":

    folder_names = ["../../eval/Datasets", "../../eval/Prompt_Output"]
    output_files = ["ikb_truth.csv", "ikb_model.csv"]


    for idx in range(len(folder_names)):
        folder = folder_names[idx]
        csv_files = os.listdir(folder)
        merged_df = merge_csv_files(csv_files,folder)
        merged_df.to_csv(output_files[idx], index=False)
