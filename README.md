# Amazon Music Reviews Analysis

This project leverages Elasticsearch to efficiently store, index, and query large volumes of Amazon music review data. Elasticsearch enables fast full-text search and powerful analytical capabilities, making it ideal for handling unstructured review content. By integrating Elasticsearch, we ensure scalable performance and flexible querying for tasks such as sentiment analysis, keyword extraction, and ranking reviews based on relevance or helpfulness


# **Folder Structure**

The project is organized into a logical folder structure to keep the code, data, and outputs separate and manageable.

```
amazon-reviews-project/
├── data/raw/
│   └── Music.txt
├── config/
│   └── index-template.json
│   └── index-template-optimized.json
├── docker/
│   └── docker-compose.yml
├── ingest/
│   └── parser.py
│   └── stream_ingest.py
├── queries_output/
│   └── queries_report.csv
    └── queries_report_optimized.csv               
├── queries/
│   ├── 01_lyrics_vocals_score_gt4.json  
│   ├── 02_top_words_score_eql.json
│   ├── ...
├── run_queries.py
├── main.py
├── benchmark/
│   └── load_scenarios.py
├── scenarios_output                
├── requirements.txt                  
└── README.md                         
```

  * `data/`: Contains the raw input data.
  * `queries/`: Holds JSON files that specify the logic for each query. 
  * `queries_output/`: Stores the final output report.
  * `requirements.txt`: Lists all necessary Python dependencies.

-----

## **How to Run the Project**

Follow these simple steps to set up and run the project on your local machine.

## **1. Clone the Repository**

First, clone the project from GitHub to your local machine:

```bash
git clone https://github.com/mobinarashidi/amazon-reviews-project.git
cd amazon-reviews-project
```

## **2. Install Dependencies**

Make sure you have **Python 3.x** installed. Then, install the required libraries listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```


## **3. Run the Script**


### Step 1: Start Services

Start the elasticsearch and kibana in the background:

```bash
docker-compose up -d
```

Wait a few moments for the services to initialize. You can verify elasticsearch is running:

```bash
https://localhost:9200 
```

---
### Step 2: Preprocess the Dataset and Ingest Data 

Use the ingestion script to push data into elastic:

```bash
python ingest.py --dataset_name <dataset_name> --index_name <index_name>
```


##  Query Execution

###  Step 4: Run Queries

Execute predefined queries on the dataset:

```bash
python ingest/stream_ingest.py --dataset data/raw/Music.txt --index amazon-music-reviews
```

---

##  Performance Benchmarking

###  Step 5: Run Benchmarks

Run performance benchmarks:

```bash
load_scenarios.py
```


---
## Authors:

Mobina Rashidi (401170564)


Farbod Fattahi (402106231)
