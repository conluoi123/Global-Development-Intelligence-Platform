import pandas as pd 
import os 

# duong dan 
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
OUTPUT_CSV = os.path.join(BASE_DIR, "data", "silver", "wb_macro_clean.csv")

df = pd.read_csv(SILVER_DIR + "/wb_macro_data.csv")
print(f"[RAW] Số dòng ban đầu: {len(df)}")
print(f"[RAW] Số cột: {list(df.columns)}")
print(f"[RAW] Số quốc gia / khu vực: {df['country_name'].nunique()}")


'''
    Phân chia thành các nhóm khu vực dựa trên 
        - Khu vực địa lý 
        - Nhóm thu nhập 
        - Nhóm vay vốn 
        - Khối kinh tế 
        - Nhóm thống kê đặc biệt
        - wld: toàn bộ tgioi 
'''
WB_REGION_CODES = {
    'AFE', 'AFW', 'ARB', 'CEB', 'CSS', 'EAP', 'EAR', 'EAS', 'ECA',
    'ECS', 'EMU', 'EUU', 'FCS', 'HIC', 'HPC', 'IBD', 'IBT', 'IDA',
    'IDB', 'IDX', 'LAC', 'LCN', 'LDC', 'LIC', 'LMC', 'LMY', 'LTE',
    'MEA', 'MIC', 'MNA', 'NAC', 'OED', 'OSS', 'PRE', 'PSS', 'PST',
    'SAR', 'SAS', 'SSA', 'SSF', 'SST', 'TEA', 'TEC', 'TLA', 'TMN',
    'TSA', 'TSS', 'UMC', 'WLD', 'XZN',
}

df_countries = df[~df['country_code'].isin(WB_REGION_CODES)].copy()
print(f"\n[STEP1] Sau lọc khu vực: {len(df_countries)} dòng")
print(f"[STEP1] Số quốc gia thật: {df_countries['country_code'].nunique()}")

# kiểm tra missing 
print(f"[MISS] Missing values per column:")
print(df_countries.isnull().sum())
missing_pct = df_countries.isnull().mean() * 100
print(missing_pct.round(1))

# lưu tạm trc khi xử lí null 
df_countries.to_csv(OUTPUT_CSV, index=False)
print(f"[SAVE] Đã lưu: {OUTPUT_CSV}")
print(f"[DONE] Shape: {df_countries.shape}")