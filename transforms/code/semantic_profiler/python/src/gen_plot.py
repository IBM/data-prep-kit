import pandas as pd
import matplotlib.pyplot as plt
import ast
import numpy as np
from config import *

plt.rcParams['figure.figsize'] = [30, 10]
plt.rcParams["font.size"] = 24

def flatten(x):
    if isinstance(x, list):
        return x
    else:
        return [x]


output_folder = "output"
for file in os.listdir(output_folder):
    path = os.path.join(output_folder, file)
    df = pd.read_csv(path)
    libs = df['libraries'].apply(lambda row: [item.strip() for item in row.split(",")])
    expanded = libs.apply(flatten)
    flattened_list = [item for sublist in expanded for item in sublist]
    all_libs = pd.Series(flattened_list)
    
    concs = df['concepts'].apply(lambda row: [item.strip() for item in row.split(",")])
    expanded = concs.apply(flatten)
    flattened_list = [item for sublist in expanded for item in sublist]
    all_concs = pd.Series(flattened_list)


    lib_counts = all_libs.value_counts()
    conc_counts = all_concs.value_counts()
    lang_counts = df['language'].value_counts()   
    ccr_counts = df['ccr'].value_counts()
  

    avg_ccr_per_language = df.groupby('language')['ccr'].mean().to_dict()
    avg_ccr_per_language = pd.Series(avg_ccr_per_language)
    

    print(lib_counts,conc_counts,lang_counts,ccr_counts)


    # library distribution
    plt.figure(figsize=(10, 6))  
    bar_width = 0.3  # Width of bars, adjust as needed
    spacing = 0.2    # Spacing between bars, smaller value increases the space
    bars = plt.bar(lib_counts[:30].index, lib_counts[:30].values, width=bar_width)
    plt.title('Package Distribution')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45, ha='right', fontsize=14)
    plt.tight_layout()
    plt.savefig('plots/package_counts_'+file.split("_")[2].strip(".csv")+'.png', format='png', dpi=300)

    rename_dict = {
    'Algorithms and Data Structures': 'Algo-DS',
    'Database Management': 'DBMS',
    'File Handling': 'Files',
    'Networking and Messaging': 'Nw-Msg',
    'Graphical User Interface Design': 'GUI',
    'Security': 'Security',
    'Scheduling and Concurrency': 'Scheduling',
    'Logging and Monitoring': 'Logging',
    'Web Development': 'WebDev',
    'Mathematics and Numerics': 'Math',
    'Code Analysis and Linting': 'CodeAnalysis',
    'Testing': 'Testing',
    'Data Serialization': 'Serialization'
    }
    # print(type(conc_counts))
    conc_counts = conc_counts.rename(rename_dict)
    conc_counts = conc_counts.drop(['Data Analysis', 'IT Automation'])

    # concept distribution
    plt.figure(figsize=(10, 6))  
    conc_counts[:-1].plot(kind='bar')
    plt.title('Semantic Concept Distribution')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45 ,ha='right', fontsize=14)
    plt.tight_layout()
    plt.savefig('plots/concept_counts_'+file.split("_")[2].strip(".csv")+'.png', format='png', dpi=300)


    #language distribution
    plt.figure(figsize=(10, 6))  
    lang_counts[:50].plot(kind='bar')
    plt.title('Language Distribution')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45, ha='right', fontsize=14)
    plt.tight_layout()
    plt.savefig('plots/lang_counts_'+file.split("_")[2].strip(".csv")+'.png', format='png', dpi=300)



    # CCR distribution
    plt.figure(figsize=(10, 6))  
    ccr_counts.plot(kind='bar')
    plt.title('Code-to-Comment Ratio Distribution')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.xticks(rotation=0, fontsize=14)
    plt.tight_layout()
    plt.savefig('plots/ccr_counts_'+file.split("_")[2].strip(".csv")+'.png', format='png', dpi=300)

    avg_ccr_per_language = avg_ccr_per_language.drop(['Perl'])
    # Avergae CCR per language
    plt.figure(figsize=(10, 6))  
    avg_ccr_per_language.plot(kind='bar')
    plt.title('Mean Code-to-Comment Ratio Per Language')
    plt.xlabel('Languages')
    plt.ylabel('Mean CCR')
    plt.xticks(rotation=45, ha='right', fontsize=14)
    plt.tight_layout()
    plt.savefig('plots/ccr_counts_langs_'+file.split("_")[2].strip(".csv")+'.png', format='png', dpi=300)


