import os
import pickle
import pandas as pd
import numpy as np

# Cấu hình các đường dẫn tương đối dựa trên cấu trúc thư mục Airflow
BASE_DIR = os.path.dirname(os.path.dirname(__file__)) # Thư mục dags/
FEATURE_STORE_PATH = os.path.join(BASE_DIR, "data", "gold", "feature_store.csv")
MODEL_PATH = os.path.join(BASE_DIR, "models", "champion_model.pkl")
PREDICTIONS_DIR = os.path.join(BASE_DIR, "data", "predictions")
OUTPUT_PATH = os.path.join(PREDICTIONS_DIR, "risk_predictions_next_year.csv")

def predict_blend(X, artifacts):
    """
    Hàm dự báo kết hợp (Weighted Blending) từ 3 mô hình XGB, LGB và CatBoost.
    """
    model_xgb = artifacts['model_xgb']
    model_lgb = artifacts['model_lgb']
    model_cat = artifacts['model_cat']
    w = artifacts['weights']
    
    # Dự đoán xác suất cho từng class từ 3 mô hình
    prob_xgb = model_xgb.predict_proba(X)
    prob_lgb = model_lgb.predict_proba(X)
    prob_cat = model_cat.predict_proba(X)
    
    # Tính toán xác suất kết hợp theo trọng số tối ưu
    prob_blend = w['xgb'] * prob_xgb + w['lgb'] * prob_lgb + w['cat'] * prob_cat
    
    # Lấy nhãn lớp có xác suất cao nhất (0: Low, 1: Medium, 2: High)
    pred_blend = np.argmax(prob_blend, axis=1)
    
    return pred_blend, prob_blend

def predict_macro_risk():
    print("=== BẮT ĐẦU DỰ BÁO RỦI RO KINH TẾ VĨ MÔ (T+1) ===")
    
    # 1. Kiểm tra sự tồn tại của dữ liệu và mô hình
    if not os.path.exists(FEATURE_STORE_PATH):
        raise FileNotFoundError(f"Không tìm thấy Feature Store tại: {FEATURE_STORE_PATH}")
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Không tìm thấy Champion Model tại: {MODEL_PATH}")
        
    # 2. Load Feature Store & Champion Model
    df_features = pd.read_csv(FEATURE_STORE_PATH)
    
    with open(MODEL_PATH, 'rb') as f:
        model_artifacts = pickle.load(f)
        
    feature_cols = model_artifacts['feature_cols']
    print(f"-> Đã load mô hình. Số lượng đặc trưng yêu cầu: {len(feature_cols)}")
    
    # 3. Lấy dữ liệu của năm gần nhất có trong hệ thống (năm t) để dự báo cho năm sau (t+1)
    max_year = df_features['year'].max()
    df_latest = df_features[df_features['year'] == max_year].copy()
    print(f"-> Dự báo rủi ro cho năm {max_year + 1} dựa trên dữ liệu năm {max_year} ({len(df_latest)} quốc gia)")
    
    # 4. Kiểm tra sự đồng bộ các cột đặc trưng giữa dữ liệu và mô hình
    missing_cols = [col for col in feature_cols if col not in df_latest.columns]
    if missing_cols:
        raise ValueError(f"Dữ liệu Feature Store thiếu các cột đặc trưng yêu cầu bởi Model: {missing_cols}")
        
    X_latest = df_latest[feature_cols]
    
    # 5. Chạy dự báo Weighted Blend
    preds, probs = predict_blend(X_latest, model_artifacts)
    
    # 6. Tổng hợp kết quả dự báo
    df_results = pd.DataFrame({
        'country_code': df_latest['country_code'],
        'country_name': df_latest['country_name'],
        'base_year': df_latest['year'],
        'target_year': df_latest['year'] + 1,
        'predicted_risk_level': preds,
        'prob_low_risk': probs[:, 0],
        'prob_med_risk': probs[:, 1],
        'prob_high_risk': probs[:, 2]
    })
    
    # Sắp xếp kết quả dự báo: ưu tiên quốc gia có mức rủi ro cao nhất và xác suất rủi ro cao nhất lên đầu
    df_results = df_results.sort_values(by=['predicted_risk_level', 'prob_high_risk'], ascending=[False, False])
    
    # 7. Lưu kết quả dự báo xuống đĩa
    os.makedirs(PREDICTIONS_DIR, exist_ok=True)
    df_results.to_csv(OUTPUT_PATH, index=False)
    
    print(f"=== DỰ BÁO HOÀN TẤT! ===")
    print(f"✅ Đã lưu kết quả dự báo tại: {OUTPUT_PATH}")
    print("\nTop 10 quốc gia có rủi ro cao nhất trong năm tới:")
    print(df_results.head(10)[['country_name', 'target_year', 'predicted_risk_level', 'prob_high_risk']])

if __name__ == "__main__":
    predict_macro_risk()
