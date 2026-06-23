import os
import pandas as pd
import numpy as np

# Cấu hình đường dẫn
BASE_DIR = os.path.dirname(os.path.dirname(__file__)) # Thư mục dags/
INPUT_CSV = os.path.join(BASE_DIR, "data", "silver", "wb_macro_clean.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "data", "gold", "feature_store.csv")

def run_feature_engineering():
    print("=== BẮT ĐẦU TẠO FEATURES (GOLD LAYER) ===")
    
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Không tìm thấy dữ liệu Silver tại: {INPUT_CSV}")
        
    df_clean = pd.read_csv(INPUT_CSV)
    
    # 1. Sắp xếp lại theo quốc gia và năm để đảm bảo tính toán lag/rolling đúng thứ tự
    df_features = df_clean.sort_values(by=['country_code', 'year']).reset_index(drop=True)
    
    # Lấy danh sách các cột numeric gốc cần tính toán lag
    numeric_cols = [c for c in df_features.columns if c not in ['country_code', 'country_name', 'year']]
    print(f"-> Các cột gốc để tạo lag: {numeric_cols}")
    
    # ==========================================================================
    # NHÓM 1: LAG & MOMENTUM (Gia tốc & Quán tính kinh tế)
    # ==========================================================================
    print("-> Đang tạo Lag & Momentum Features...")
    for col in numeric_cols:
        df_features[f'{col}_lag1'] = df_features.groupby('country_code')[col].shift(1)
        df_features[f'{col}_lag2'] = df_features.groupby('country_code')[col].shift(2)
        
    # Gia tốc thay đổi so với năm ngoái (Momentum)
    df_features['inflation_acceleration'] = df_features['inflation'] - df_features['inflation_lag1']
    df_features['gdp_growth_change'] = df_features['gdp_growth'] - df_features['gdp_growth_lag1']
    if 'external_debt' in df_features.columns:
        df_features['debt_change'] = df_features['external_debt'] - df_features['external_debt_lag1']
        
    # ==========================================================================
    # NHÓM 2: VOLATILITY & TREND (Xu hướng và độ biến động dài hạn)
    # ==========================================================================
    print("-> Đang tạo Volatility & Trend Features...")
    # Thêm external_debt vào nhóm tính volatility nếu cột này tồn tại
    target_cols = ['gdp_growth', 'inflation', 'fdi_inflow']
    if 'external_debt' in df_features.columns:
        target_cols.append('external_debt')
        
    for col in target_cols:
        # Độ biến động trong 3 năm gần nhất (Volatility)
        df_features[f'{col}_volatility_3y'] = df_features.groupby('country_code')[col].transform(
            lambda x: x.rolling(window=3, min_periods=1).std()
        )
        # Đường trung bình 5 năm (SMA 5)
        df_features[f'{col}_sma5'] = df_features.groupby('country_code')[col].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        # Độ lệch so với xu hướng trung bình 5 năm
        df_features[f'{col}_vs_trend'] = df_features[col] - df_features[f'{col}_sma5']
        
    # ==========================================================================
    # NHÓM 3: COMPOSITE INDICES (Các chỉ số khốn khổ và tổn thương bên ngoài)
    # ==========================================================================
    print("-> Đang tạo Composite Indices...")
    # Chỉ số khốn khổ Okun (Misery Index = Lạm phát + Thất nghiệp) - giữ nguyên định nghĩa gốc không scale
    if 'unemployment' in df_features.columns:
        df_features['misery_index'] = df_features['inflation'] + df_features['unemployment']
        
    # Độ tổn thương bên ngoài (External Vulnerability = Cán cân vãng lai - FDI)
    if 'current_account' in df_features.columns:
        df_features['external_vulnerability'] = df_features['current_account'] - df_features['fdi_inflow']
        
    # ==========================================================================
    # NHÓM 4: GLOBAL Z-SCORE (Chuẩn hóa chéo quốc gia theo từng năm)
    # ==========================================================================
    print("-> Đang tạo Cross-country Z-Scores theo năm...")
    df_features['gdp_global_zscore'] = df_features.groupby('year')['gdp_growth'].transform(
        lambda x: (x - x.mean()) / x.std()
    )
    df_features['inflation_global_zscore'] = df_features.groupby('year')['inflation'].transform(
        lambda x: (x - x.mean()) / x.std()
    )
    
    # ==========================================================================
    # NHÓM 5: OUTLIERS CLIPPING (Đồng bộ hóa 2 biến clipped cho Feature Store)
    # ==========================================================================
    print("-> Đang tạo Clipped Features cho Lạm phát và FDI...")
    df_features['inflation_clipped'] = df_features['inflation'].clip(upper=100, lower=-10)
    if 'fdi_inflow' in df_features.columns:
        df_features['fdi_inflow_clipped'] = df_features['fdi_inflow'].clip(upper=30, lower=-10)

    # ==========================================================================
    # BƯỚC CUỐI: LOẠI BỎ CÁC DÒNG RỖNG DO LAG & ROLLING (Sửa lỗi fillna(0) cũ)
    # ==========================================================================
    # Lệnh dropna sẽ xóa bỏ các dòng bị NaN ở 4 năm đầu của mỗi quốc gia, đảm bảo data huấn luyện sạch
    initial_rows = len(df_features)
    df_features = df_features.dropna().reset_index(drop=True)
    final_rows = len(df_features)
    print(f"-> Đã loại bỏ các dòng bị NaN ở đầu chuỗi thời gian. Số dòng giảm: {initial_rows - final_rows} dòng.")
    
    # Lưu file Gold Feature Store
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df_features.to_csv(OUTPUT_CSV, index=False)
    
    print(f"✅ Đã lưu Gold Feature Store thành công tại: {OUTPUT_CSV}")
    print(f"=== HOÀN THÀNH GOLD LAYER (Shape: {df_features.shape}) ===")

if __name__ == "__main__":
    run_feature_engineering()
