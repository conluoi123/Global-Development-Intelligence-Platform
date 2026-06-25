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
    df_risk = pd.read_csv(PREDICTIONS)
    for _, r in df_risk.iterrows():
        level = risk_map.get(int(r['predicted_risk_level']), "N/A")
        text = (
            f"Dự báo rủi ro kinh tế {r['country_name']} ({r['country_code']}) "
            f"năm {int(r['target_year'])}: mức độ rủi ro {level}"
            f"Xác suất rủi ro cao: {float(r['prob_high_risk']):.1%}, "
            f"trung bình: {float(r['prob_med_risk']):.1%}, "
            f"thấp: {float(r['prob_low_risk']):.1%}. "
            f"Dựa trên dữ liệu năm {int(r['base_year'])}."
        )
        
        docs.append({
            "text": text,
            "source": "risk_prediction", 
            "country_code": str(r['country_code']), 
            "country_name" : str(r['country_name']), 
            "year": int(r['target_year']), 
            "risk_level": level,
            "prob_high": round(float(r["prob_high_risk"]), 4)
        })

    print(f"Built {len(docs)} documents from risk predictions.")

    # macro forecast (chưa chính xác lắm cho 2025-2029)
    df_fc = pd.read_csv(FORECAST)
    indicator_vi = {
        "gdp_growth": "tăng turonwgr GDP", 
        "inflation": "lạm phát", 
        "fdi_inflow": "dòng vốn FDI (% GDP)",
    }
    n_fc = 0 
    for (code, name, year), grp in df_fc.groupby(["country_code", "country_name", "year"]):
        parts = [
            f"{indicator_vi.get(r['indicator'], r['indicator'])}: {r['forecast_value']:.2f}%"
            for _, r in grp.iterrows()
        ]
        text = (
            f"Dự báo kinh tế vĩ mô {name} ({code}) năm {int(year)}: "
            + ", ".join(parts) + "."
        )
        docs.append({
            "text": text,
            "source": "macro_forecast",
            "country_code": str(code),
            "country_name": str(name),
            "year": int(year),
        })
        n_fc += 1
    
    print(f"Built {n_fc} documents from macro forecast.")
    
    # feature snapshot 
    df_feat = pd.read_csv(FEATURES)
    latest = df_feat["year"].max()
    df_snap = df_feat[df_feat["year"]==latest]

    col_vi = { 
        "gdp_growth":          "tăng trưởng GDP",
        "inflation":           "lạm phát",
        "unemployment":        "thất nghiệp",
        "fdi_inflow":          "FDI ròng (% GDP)",
        "trade_balance_pct_gdp": "cán cân thương mại (% GDP)",
        "govt_debt_pct_gdp":   "nợ chính phủ (% GDP)",
    }
    available = [c for c in col_vi if c in df_snap.columns]
    n_snap = 0
    for _, row in df_snap.iterrows():
        stats = ", ".join(f"{col_vi[c]}: {row[c]:.2f}" for c in available)
        text = (
            f"Chỉ số kinh tế {row['country_name']} ({row['country_code']}) "
            f"năm {int(latest)}: {stats}."
        )
        docs.append({
            "text": text,
            "source": "feature_snapshot",
            "country_code": str(row["country_code"]),
            "country_name": str(row["country_name"]),
            "year": int(latest),
        })
        n_snap += 1
    print(f"Built {n_snap} documents from feature snapshot.")
    return docs


def build_index(docs: list[dict]): 
    os.makedirs(INDEX_DIR, exist_ok=True)
    print(f" Tải embedding model: {EMBED_MODEL}")
    model = SentenceTransformer(EMBED_MODEL)

    texts = [PASSAGE_PREFIX + d["text"] for d in docs]
    print(f"Đang nhúng {len(texts)} documents (batch_size=64)...")
    embeddings = model.encode(
        texts, 
        batch_size=64,
        show_progress_bar=True, 
        normalize_embeddings=True, 
        convert_to_numpy=True, 
    ).astype(np.float32)

    # indexFlatIP 
    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)

    faiss.write_index(index, FAISS_PATH)
    
    print(f"  FAISS index → {FAISS_PATH}  ({index.ntotal} vectors, dim={dim})")
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
    print(f"  Metadata     → {META_PATH}")

if __name__ == "__main__":
    print("=== BUILD GDIP KNOWLEDGE BASE ===\n")
    print(" Sinh documents từ CSV...")
    documents = build_documents()
    print(f"\n  → Tổng: {len(documents)} documents")
    build_index(documents)
    print("\n=== HOÀN THÀNH ✓ ===")