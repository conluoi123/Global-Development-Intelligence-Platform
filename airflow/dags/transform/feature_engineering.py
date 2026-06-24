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
    
    df_features = df_clean.sort_values(by=['country_code', 'year']).reset_index(drop=True)
    
    # thập kỷ
    df_features['decade'] = (df_features['year'] // 10) * 10
    
    # Lấy danh sách các cột numeric gốc cần tính toán lag
    numeric_cols = [c for c in df_features.columns if c not in ['country_code', 'country_name', 'year', 'decade']]
    print(f"-> Các cột gốc để tạo lag: {numeric_cols}")
    
    '''
        lag giúp mô hình học được năm nay nền kinh tế đứng trên nền tảng của những năm trước như thế nào (risk persistance - rủi ro kéo dài)
        momentum (gia tốc) giúp mô hình hiểu được lạm phát đang cao hay thấp, tăng nhanh hay giảm nhanh 
    '''
    print("-> Đang tạo Lag & Momentum Features...")
    for col in numeric_cols:
        df_features[f'{col}_lag1'] = df_features.groupby('country_code')[col].shift(1)
        df_features[f'{col}_lag2'] = df_features.groupby('country_code')[col].shift(2)
       
    df_features['inflation_acceleration'] = df_features['inflation'] - df_features['inflation_lag1']
    df_features['gdp_growth_change'] = df_features['gdp_growth'] - df_features['gdp_growth_lag1']

    if 'external_debt' in df_features.columns:
        '''
            tốc độ tăng của nợ nước ngoài so với năm trước
            giúp mô hình hiểu được rủi ro nợ có đang gia tăng hay giảm 
        '''
        df_features['debt_change'] = df_features['external_debt'] - df_features['external_debt_lag1']


    '''
        - Độ biến động trong 3 năm gần nhất (Volatility) [độ lệch chuẩn 3 năm gần nhất] -> độ bất ổn của nền kinh tế, thường thì một nền kinh tế ổn định sẽ ít khủng hoảng hơn một nền kinh tế biến động 

        - Đường trung bình 5 năm (SMA 5) [trung bình của 5 năm gần nhất] ->  xem quốc gia đang lệch khỏi quỹ đạo bn (2 qg có cùng tbinh GDP la 6%, trong năm 2026 một cái là 2% một cái là 6% thì hai cái này nó khác nhau hoàn toàn)
        - Độ lệch so với xu hướng trung bình 5 năm (Trend Deviation) -> [)
    '''
    print("-> Đang tạo Volatility & Trend Features...")
    target_cols = ['gdp_growth', 'inflation', 'fdi_inflow']
    if 'external_debt' in df_features.columns:
        target_cols.append('external_debt')
        
    for col in target_cols:
        df_features[f'{col}_volatility_3y'] = df_features.groupby('country_code')[col].transform(
            lambda x: x.rolling(window=3, min_periods=1).std()
        )
        df_features[f'{col}_sma5'] = df_features.groupby('country_code')[col].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        df_features[f'{col}_vs_trend'] = df_features[col] - df_features[f'{col}_sma5']
        
    '''
        Misery Index: được đề xuất bởi nhà Kinh tế học Arthur Okun 
        Feature này tổng hợp hai chỉ số lạm phát và thất nghiệp -> giúp mô hình học được nền kinh tế từ góc độ tổng hợp thay vì chỉ nhìn vào một con số riêng lẻ 
    '''
    print("-> Đang tạo Composite Indices...")
    if 'unemployment' in df_features.columns:
        df_features['misery_index'] = df_features['inflation'] + df_features['unemployment']
        
    
    '''
        Current Account = tiền vào - tiền ra 
        > 0: thặng dư 
        < 0: thâm hụt 

        hai qgia có current_account như nhau nhma khác fdi_inflow cũng dẫn đến kết quả khủng hoảng khác nhau. Mô hình sẽ trloi được câu hỏi có đang phụ thuộc quá mức vào nguồn tiền bên ngoài hay không ? 
    '''
    if 'current_account' in df_features.columns:
        df_features['external_vulnerability'] = df_features['current_account'] - df_features['fdi_inflow']
        
    '''
        GLOBAL Z-SCORE (Chuẩn hóa chéo quốc gia theo từng năm): chuẩn hóa về phân phối chuẩn. Giúp mô hình biết được rằng một nền kinh tế có tỷ lệ lạm phát là 10% ở năm bình thường sẽ khác gì nếu ở một năm mà kinh tế toàn cầu đang bị lạm phát => Giúp mô hình hiểu được bối cảnh của thế giới.
    '''
    print("-> Đang tạo Cross-country Z-Scores theo năm...")
    df_features['gdp_global_zscore'] = df_features.groupby('year')['gdp_growth'].transform(
        lambda x: (x - x.mean()) / x.std()
    )
    df_features['inflation_global_zscore'] = df_features.groupby('year')['inflation'].transform(
        lambda x: (x - x.mean()) / x.std()
    )
    
    '''
        World Bank có nhiều outliers, nên dùng clipped này để chuẩn hóa về dạng chuẩn cho dễ scale 
    '''
    print("-> Đang tạo Clipped Features cho Lạm phát và FDI...")
    df_features['inflation_clipped'] = df_features['inflation'].clip(upper=100, lower=-10)
    if 'fdi_inflow' in df_features.columns:
        df_features['fdi_inflow_clipped'] = df_features['fdi_inflow'].clip(upper=30, lower=-10)

    '''
        loại bỏ dòng NaN ở đầu chuỗi thời gian
    '''
    initial_rows = len(df_features)
    df_features = df_features.dropna().reset_index(drop=True)
    final_rows = len(df_features)
    print(f"-> Đã loại bỏ các dòng bị NaN ở đầu chuỗi thời gian. Số dòng giảm: {initial_rows - final_rows} dòng.")
    
    # save 
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df_features.to_csv(OUTPUT_CSV, index=False)
    
    print(f"Đã lưu Gold Feature Store thành công tại: {OUTPUT_CSV}")
    print(f"=== HOÀN THÀNH GOLD LAYER (Shape: {df_features.shape}) ===")

if __name__ == "__main__":
    run_feature_engineering()
