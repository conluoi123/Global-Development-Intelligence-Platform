import os
import pickle
import pandas as pd
from sklearn.impute import KNNImputer

# Cấu hình đường dẫn tuyệt đối dựa trên cấu trúc thư mục Airflow
BASE_DIR = os.path.dirname(os.path.dirname(__file__)) # Thư mục dags/
INPUT_CSV = os.path.join(BASE_DIR, "data", "silver", "wb_macro_data.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "data", "silver", "wb_macro_clean.csv")
IMPUTER_PATH = os.path.join(BASE_DIR, "transform", "knn_imputer.pkl") 

# Các mã khu vực/nhóm kinh tế cần loại bỏ (chỉ giữ quốc gia thật)
WB_REGION_CODES = {
    'AFE', 'AFW', 'ARB', 'CEB', 'CSS', 'EAP', 'EAR', 'EAS', 'ECA',
    'ECS', 'EMU', 'EUU', 'FCS', 'HIC', 'HPC', 'IBD', 'IBT', 'IDA',
    'IDB', 'IDX', 'LAC', 'LCN', 'LDC', 'LIC', 'LMC', 'LMY', 'LTE',
    'MEA', 'MIC', 'MNA', 'NAC', 'OED', 'OSS', 'PRE', 'PSS', 'PST',
    'SAR', 'SAS', 'SSA', 'SSF', 'SST', 'TEA', 'TEC', 'TLA', 'TMN',
    'TSA', 'TSS', 'UMC', 'WLD', 'XZN',
}

def clean_and_impute_data():
    print("=== BẮT ĐẦU CLEAN & IMPUTE (SILVER LAYER) ===")
    
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Không tìm thấy file input tại: {INPUT_CSV}")
        
    df = pd.read_csv(INPUT_CSV)
    print(f"-> Số dòng ban đầu: {len(df)}")
    
    # lọc 
    df_countries = df[~df['country_code'].isin(WB_REGION_CODES)].copy()
    print(f"-> Giữ lại {df_countries['country_code'].nunique()} quốc gia thực tế.")
    
    # xử lý missing 
    cols_to_drop = ['exchange_rate', 'total_reserves']
    df_cleaned = df_countries.drop(columns=cols_to_drop, errors='ignore')
    
    # điền ffill 
    df_cleaned = df_cleaned.groupby('country_code', group_keys=False).apply(lambda x: x.ffill())
    
    # KNNImputer 
    numeric_cols = [c for c in df_cleaned.columns if c not in ['country_code', 'country_name', 'year']]
    
    # kiểm tra model imputer 
    if os.path.exists(IMPUTER_PATH):
        print(f"-> Phát hiện Imputer đã tồn tại. Đang load: {IMPUTER_PATH}")
        with open(IMPUTER_PATH, 'rb') as f:
            imputer = pickle.load(f)
    else:
        print("-> Không tìm thấy Imputer đã lưu. Đang fit Imputer mới trên dữ liệu Train (year < 2011)...")
        train_period = df_cleaned[df_cleaned['year'] < 2011]
        imputer = KNNImputer(n_neighbors=5)
        imputer.fit(train_period[numeric_cols])
        
        # lưu lại để tái sử dụng 
        os.makedirs(os.path.dirname(IMPUTER_PATH), exist_ok=True)
        with open(IMPUTER_PATH, 'wb') as f:
            pickle.dump(imputer, f)
        print(f"✅ Đã lưu Imputer mới tại: {IMPUTER_PATH}")
        
    # Điền khuyết bằng Imputer
    df_cleaned[numeric_cols] = imputer.transform(df_cleaned[numeric_cols])
    
    # khôi phục các cột metadata định danh
    df_cleaned['country_code'] = df_countries['country_code']
    df_cleaned['country_name'] = df_countries['country_name']
    df_cleaned['year'] = df_countries['year']
    
    # sắp xếp lại cột 
    cols_ordered = ['country_code', 'country_name', 'year'] + [c for c in df_cleaned.columns if c not in ['country_code', 'country_name', 'year']]
    df_cleaned = df_cleaned[cols_ordered]
    
    # kiểm tra lại missing 
    missing_count = df_cleaned.isnull().sum().sum()
    print(f"-> Kiểm tra dữ liệu sau xử lý: còn {missing_count} dòng trống.")
    
    # lưu file 
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df_cleaned.to_csv(OUTPUT_CSV, index=False)
    print(f" Đã lưu dữ liệu Silver sạch tại: {OUTPUT_CSV}")
    print(f"=== HOÀN THÀNH SILVER LAYER (Shape: {df_cleaned.shape}) ===")

if __name__ == "__main__":
    clean_and_impute_data()
