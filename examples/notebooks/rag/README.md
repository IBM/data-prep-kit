# RAG with Data Prep Kit

This folder has examples of RAG applications with data prep kit (DPK).

## Step-0: Getting Started

```bash
## Clone this repo
git  clone   https://github.com/IBM/data-prep-kit

cd data-prep-kit
## All commands from here on out, assume you are in project root directory
```

## Step-1: Setup Python Environment

[setup-python-dev-env.md](./setup-python-dev-env.md)

Make sure Jupyter is running after this step.  We will use this Jupyter instance to run the notebooks in next steps.

## RAG Workflow

Here is the overall work flow.  For details see [RAG-explained](./RAG-explained.md)

![](media/rag-overview-2.png)

## Step-2: Process Input Documents (RAG stage 1, 2 & 3)

This code uses DPK to 

- Extract text from PDFs (RAG stage-1)
- Performs de-dupes (RAG stage-1)
- split the documents into chunks (RAG stage-2)
- vectorize the chunks (RAG stage-3)

Here is the code: 

- Python version: TODO
- Ray version: [rag_1A_dpk_process_ray.ipynb](rag_1A_dpk_process_ray.ipynb)


## Step-3: Load data into vector database  (RAG stage 4)

Our vector database is [Milvus](https://milvus.io/)

Code: [rag_1B_load_data_into_milvus.ipynb](rag_1B_load_data_into_milvus.ipynb)


## Step-4: Perform vector search (RAG stage 5 & 6)

Let's do a few searches on our data.

Code: [rag_1C_vector_search.ipynb](rag_1C_vector_search.ipynb)


## Step-5: Query the documents using LLM (RAG steps 5, 6, 7, 8 & 9)

We will use **Llama** as our LLM running on [Replicate](https://replicate.com/) service.


### 5.1 - Create an `.env` file

To use replicate service, we will need a replicate API token.

You can get one from [Replicate](https://replicate.com/)

Create an `.env` file (notice the dot in the file name in this directory with content like this

```text
REPLICATE_API_TOKEN=your REPLICATE token goes here
```

### 5.2 - Run the query code

Code: [rag_1D_query_llama_replicate.ipynb](rag_1D_query_llama_replicate.ipynb)



## Step 6: Illama Index

For comparision, we can use [Llama-index](https://docs.llamaindex.ai/) framework to process PDFs and query

### Step 6.1 - Process documents and save the index into vector DB

Code: [rag_2A_llamaindex_process.ipynb](rag_2A_llamaindex_process.ipynb)

### Step 6.2 - Query documents with LLM

code: [rag_2B_llamaindex_query.ipynb](rag_2B_llamaindex_query.ipynb)