'''
    bUILD faiss KNOWLEDGE 
'''

import os 
import json
import numpy as np 
import pandas as pd 
from sentence_transformers import SentenceTransformer
import faiss 

# config 
ROOT_DIR = dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "airflow", "dags", "data")
INDEX_DIR = os.path.join(ROOT_DIR, "rag", "index")
PREDICTIONS = os.path.join(DATA_DIR, "predictions", "risk_predictions_next_year.csv")
FORECAST = os.path.join(DATA_DIR, "gold", "global_forecast_2025_2029.csv")
FEATURES = os.path.join(DATA_DIR, "gold", "feature_store.csv")

FAISS_PATH = os.path.join(INDEX_DIR, "faiss.index")
META_PATH = os.path.join(INDEX_DIR, "metadata.json")

# sd model multilingual-e5 -> bắt buộc prefix "passage: " khi encode documents 
EMBED_MODEL = "intfloat/multilingual-e5-base"
PASSAGE_PREFIX = "passage: "

def build_documents() -> list[dict]: 
    docs = []
    risk_map =  {0: "THẤP", 1: "TRUNG BÌNH", 2: "CAO"}

    # risk 
    