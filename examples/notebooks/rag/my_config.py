import os 


## Configuration
class MyConfig:
    pass 

MY_CONFIG = MyConfig ()

## Input Data - configure this to the folder we want to process
MY_CONFIG.INPUT_DATA_DIR = "input"
MY_CONFIG.OUTPUT_FOLDER = "output"
MY_CONFIG.OUTPUT_FOLDER_FINAL = os.path.join(MY_CONFIG.OUTPUT_FOLDER , "output_final")
### -------------------------------

### Milvus config
MY_CONFIG.DB_URI = './rag_1_dpk.db'  # For embedded instance
MY_CONFIG.COLLECTION_NAME = 'dpk_papers'


## Embedding model
MY_CONFIG.EMBEDDING_MODEL = 'sentence-transformers/all-MiniLM-L6-v2'
MY_CONFIG.EMBEDDING_LENGTH = 384

## LLM Model
MY_CONFIG.LLM_MODEL = "meta/meta-llama-3-8b-instruct"



## RAY CONFIGURATION
num_cpus_available =  os.cpu_count()
# print (num_cpus_available)
# MY_CONFIG.RAY_NUM_CPUS = num_cpus_available // 2  ## use half the available cores for processing
MY_CONFIG.RAY_NUM_CPUS =  0.8
# print (MY_CONFIG.RAY_NUM_CPUS)
MY_CONFIG.RAY_MEMORY_GB = 2  # GB
# MY_CONFIG.RAY_RUNTIME_WORKERS = num_cpus_available // 3
MY_CONFIG.RAY_RUNTIME_WORKERS = 2