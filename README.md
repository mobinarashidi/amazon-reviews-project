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
### Step 2: Indexing Data

This step defines how we structure and optimize the indexing of Amazon Music review data in Elasticsearch using Kibana Dev Tools.

#### Initial Index Template (Before Optimization)
```bash
PUT _index_template/amazon-reviews-template
{
  "index_patterns": ["amazon-music-*"],
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "productId": { "type": "keyword" },
        "userId": { "type": "keyword" },
        "score": { "type": "float" },
        "helpfulness": { "type": "float" },
        "time": { "type": "long" },
        "summary_text": { "type": "text" },
        "review_text": { "type": "text" }
      }
    }
  }
}

```
To improve search capabilities, we delete the existing index and redefine the template with enhanced analysis:
```bash
DELETE amazon-music-reviews
```
#### Optimized Index Template (After)
The updated template introduces:

- A custom shingle analyzer for phrase-based search and similarity matching

- Multi-field mappings for summary_text and review_text, including:

     - keyword for exact match filtering

     - shingles for advanced text analysis
```bash
PUT _index_template/amazon-reviews-template
{
  "index_patterns": ["amazon-music-*"],
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "analysis": {
        "analyzer": {
          "shingle_analyzer": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": ["lowercase", "shingle"]
          }
        }
      }
    },
    "mappings": {
      "properties": {
        "productId": {
          "type": "keyword"
        },
        "userId": {
          "type": "keyword"
        },
        "score": {
          "type": "float"
        },
        "helpfulness": {
          "type": "float"
        },
        "time": {
          "type": "long"
        },
        "summary_text": {
          "type": "text",
          "fields": {
            "keyword": { "type": "keyword" }
          }
        },
        "review_text": {
          "type": "text",
          "fields": {
            "keyword": { "type": "keyword" },
            "shingles": {
              "type": "text",
              "analyzer": "shingle_analyzer"
            }
          }
        }
      }
    }
  }
}


```


---
### Step 3: Preprocess the Dataset and Ingest  

Use the ingestion script to push data into elastic:

```bash
python ingest/stream_ingest.py --dataset data/raw/Music.txt --index amazon-music-reviews
```

##  Query Execution

###  Step 4: Run Queries

Execute predefined queries on the dataset:

```bash
python run_queries.py
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

Arian Afzalzade (401105572)
