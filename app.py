import os
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import pickle
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import shap
import matplotlib.pyplot as plt

# ==============================================================================
# CẤU HÌNH GIAO DIỆN STREAMLIT
# ==============================================================================
st.set_page_config(
    page_title="GDIP - World Economic AI Analyst",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Styling CSS để giao diện trông premium và hiện đại
st.markdown("""
    <style>
    .main { background-color: #0f1116; color: #e2e8f0; }
    .stMetric { background-color: #1a202c; border-radius: 8px; padding: 15px; border: 1px solid #2d3748; }
    div[data-testid="stSidebar"] { background-color: #171923; }
    h1, h2, h3 { color: #f7fafc; font-family: 'Outfit', sans-serif; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #1a202c; border: 1px solid #2d3748; border-radius: 4px 4px 0px 0px; padding: 10px 20px;
    }
    .stTabs [aria-selected="true"] { background-color: #3182ce !important; color: white !important; }
    </style>
""", unsafe_allow_html=True)

# ==============================================================================
# TẢI DỮ LIỆU & MODEL
# ==============================================================================
FEATURE_STORE_PATH = "airflow/dags/data/gold/feature_store.csv"
MODEL_PATH = "airflow/dags/models/champion_model.pkl"

@st.cache_data
def load_data():
    if not os.path.exists(FEATURE_STORE_PATH):
        st.error(f"Không tìm thấy Feature Store tại: {FEATURE_STORE_PATH}. Vui lòng kiểm tra lại đường dẫn.")
        return None
    df = pd.read_csv(FEATURE_STORE_PATH)
    return df

@st.cache_resource
def load_model():
    if not os.path.exists(MODEL_PATH):
        st.error(f"Không tìm thấy Champion Model tại: {MODEL_PATH}.")
        return None
    with open(MODEL_PATH, 'rb') as f:
        artifacts = pickle.load(f)
    return artifacts

df_features = load_data()
model_artifacts = load_model()

# ==============================================================================
# HÀM DỰ BÁO WEIGHTED BLEND
# ==============================================================================
def predict_blend(X, artifacts):
    model_xgb = artifacts['model_xgb']
    model_lgb = artifacts['model_lgb']
    model_cat = artifacts['model_cat']
    w = artifacts['weights']
    
    # Lấy xác suất của từng mô hình
    prob_xgb = model_xgb.predict_proba(X)
    prob_lgb = model_lgb.predict_proba(X)
    prob_cat = model_cat.predict_proba(X)
    
    # Tổ hợp theo trọng số
    prob_blend = w['xgb'] * prob_xgb + w['lgb'] * prob_lgb + w['cat'] * prob_cat
    pred_blend = np.argmax(prob_blend, axis=1)
    
    return pred_blend, prob_blend

# ==============================================================================
# MAIN APP
# ==============================================================================
if df_features is not None and model_artifacts is not None:
    feature_cols = model_artifacts['feature_cols']
    
    # Đầu trang / Header
    st.title("📈 Global Development Intelligence Platform (GDIP)")
    st.subheader("Economic AI Analyst & Early Warning Crisis System")
    st.markdown("Hệ thống AI phân tích sức khỏe tài chính vĩ mô toàn cầu và cảnh báo sớm rủi ro suy thoái kinh tế (T+1).")
    st.write("---")

    # SIDEBAR CẤU HÌNH DỰ BÁO
    st.sidebar.header("⚙️ Cấu hình Dự báo")
    
    # 1. Chọn năm cơ sở để dự báo
    available_years = sorted(df_features['year'].unique(), reverse=True)
    selected_year = st.sidebar.selectbox("Chọn năm cơ sở (t) để dự báo cho năm sau (t+1):", available_years, index=0)
    
    # 2. Chọn quốc gia để phân tích sâu
    all_countries = sorted(df_features['country_name'].unique())
    selected_country = st.sidebar.selectbox("Chọn quốc gia phân tích sâu (Deep-Dive):", all_countries, index=all_countries.index("Vietnam") if "Vietnam" in all_countries else 0)

    # CHẠY DỰ BÁO CHO TOÀN BỘ THẾ GIỚI TẠI NĂM ĐÃ CHỌN
    df_year = df_features[df_features['year'] == selected_year].copy()
    X_year = df_year[feature_cols]
    
    preds, probs = predict_blend(X_year, model_artifacts)
    
    df_year['predicted_risk_level'] = preds
    df_year['prob_low'] = probs[:, 0]
    df_year['prob_med'] = probs[:, 1]
    df_year['prob_high'] = probs[:, 2]
    
    # CHIA CÁC TAB CHỨC NĂNG
    tab_map, tab_detail = st.tabs(["🌍 Bản đồ Rủi ro Toàn cầu", "🔍 Phân tích chi tiết Quốc gia"])

    # ==========================================================================
    # TAB 1: BẢN ĐỒ RỦI RO TOÀN CẦU
    # ==========================================================================
    with tab_map:
        st.header(f"Bản đồ Cảnh báo Sớm rủi ro năm {selected_year + 1}")
        st.write(f"Dữ liệu phân tích dựa trên các chỉ số vĩ mô năm {selected_year}.")
        
        # Hộp thông tin KPI tổng quan
        num_countries = len(df_year)
        num_high = (preds == 2).sum()
        num_med = (preds == 1).sum()
        
        col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
        col_kpi1.metric("Tổng số quốc gia phân tích", f"{num_countries}", help="Tổng số quốc gia có đủ dữ liệu năm nay")
        col_kpi2.metric("🔴 Quốc gia Cảnh báo Đỏ (High Risk)", f"{num_high}", f"{num_high/num_countries*100:.1f}% tổng số")
        col_kpi3.metric("🟡 Quốc gia Cảnh báo Vàng (Medium)", f"{num_med}", f"{num_med/num_countries*100:.1f}% tổng số")
        
        st.write("")
        
        # Vẽ Plotly Choropleth Map
        # Định nghĩa map màu: 0 -> Xanh, 1 -> Vàng, 2 -> Đỏ
        fig_map = px.choropleth(
            df_year,
            locations="country_code",
            color="predicted_risk_level",
            hover_name="country_name",
            hover_data={
                "predicted_risk_level": False,
                "prob_high": ":.2%",
                "prob_med": ":.2%",
                "gdp_growth": ":.2f%"
            },
            color_continuous_scale=[[0, 'green'], [0.5, 'orange'], [1.0, 'red']],
            labels={"predicted_risk_level": "Mức độ rủi ro"},
            title=f"Bản đồ phân loại rủi ro kinh tế toàn cầu năm {selected_year + 1}"
        )
        fig_map.update_layout(
            coloraxis_showscale=False,
            geo=dict(showframe=False, showcoastlines=True, projection_type='equirectangular', bgcolor='#0f1116'),
            margin={"r":0,"t":40,"l":0,"b":0},
            height=600,
            paper_bgcolor='#0f1116'
        )
        st.plotly_chart(fig_map, use_container_width=True)
        
        # Hiển thị Top 10 quốc gia có nguy cơ cao nhất
        st.write("---")
        st.subheader("🚨 Top 10 quốc gia có nguy cơ khủng hoảng/suy thoái cao nhất")
        
        top_10 = df_year.sort_values(by='prob_high', ascending=False).head(10)
        
        fig_bar = px.bar(
            top_10,
            x="prob_high",
            y="country_name",
            orientation="h",
            color="prob_high",
            color_continuous_scale="Reds",
            labels={"prob_high": "Xác suất rủi ro cao (High Risk Prob)", "country_name": "Quốc gia"},
            text_auto=".2%"
        )
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, height=400, paper_bgcolor='#0f1116', plot_bgcolor='#1a202c')
        st.plotly_chart(fig_bar, use_container_width=True)

    # ==========================================================================
    # TAB 2: CHI TIẾT QUỐC GIA & SHAP EXPLANATION
    # ==========================================================================
    with tab_detail:
        st.header(f"Phân tích sâu: {selected_country}")
        
        # Lọc dữ liệu của quốc gia được chọn
        df_country_all = df_features[df_features['country_name'] == selected_country].sort_values(by='year')
        df_country_year = df_year[df_year['country_name'] == selected_country]
        
        if df_country_year.empty:
            st.warning(f"Không có dữ liệu năm {selected_year} cho {selected_country}.")
        else:
            # 1. Hiển thị kết quả dự báo chi tiết
            risk_val = df_country_year['predicted_risk_level'].values[0]
            prob_low_val = df_country_year['prob_low'].values[0]
            prob_med_val = df_country_year['prob_med'].values[0]
            prob_high_val = df_country_year['prob_high'].values[0]
            
            risk_label = "🔴 NGUY CƠ CAO (High Risk)" if risk_val == 2 else "🟡 RỦI RO TRUNG BÌNH (Medium Risk)" if risk_val == 1 else "🟢 RỦI RO THẤP (Low Risk)"
            
            col_res1, col_res2 = st.columns([1, 2])
            
            with col_res1:
                st.write("")
                st.markdown(f"#### Dự báo rủi ro năm {selected_year + 1}:")
                st.markdown(f"### {risk_label}")
                st.write("---")
                st.write(f"📊 Phân phối xác suất từ AI:")
                st.write(f"- Thấp (Low): `{prob_low_val:.2%}`")
                st.write(f"- Trung bình (Medium): `{prob_med_val:.2%}`")
                st.write(f"- Cao (High): `{prob_high_val:.2%}`")
            
            # Vẽ Gauge Chart thể hiện xác suất nguy cơ cao
            with col_res2:
                fig_gauge = go.Figure(go.Indicator(
                    mode = "gauge+number",
                    value = prob_high_val * 100,
                    domain = {'x': [0, 1], 'y': [0, 1]},
                    title = {'text': "Xác suất rủi ro khủng hoảng (High Risk Prob %)", 'font': {'size': 18}},
                    gauge = {
                        'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "white"},
                        'bar': {'color': "red" if risk_val == 2 else "orange" if risk_val == 1 else "green"},
                        'bgcolor': "white",
                        'borderwidth': 2,
                        'bordercolor': "gray",
                        'steps': [
                            {'range': [0, 30], 'color': 'rgba(0, 255, 0, 0.1)'},
                            {'range': [30, 70], 'color': 'rgba(255, 165, 0, 0.1)'},
                            {'range': [70, 100], 'color': 'rgba(255, 0, 0, 0.1)'}
                        ],
                    }
                ))
                fig_gauge.update_layout(height=280, margin=dict(t=50, b=0, l=0, r=0), paper_bgcolor='#0f1116')
                st.plotly_chart(fig_gauge, use_container_width=True)

            # 2. SHAP EXPLAINABILITY (Giải thích mô hình AI)
            st.write("---")
            st.subheader("🧠 Giải thích quyết định của AI (SHAP Local Explanation)")
            st.write("Biểu đồ thể hiện các chỉ số kinh tế vĩ mô nào đóng vai trò lớn nhất làm tăng (màu đỏ) hoặc giảm (màu xanh) mức độ rủi ro của quốc gia.")
            
            try:
                # Dùng model XGBoost (chiếm tỷ trọng chính 53%) để tính SHAP values
                model_xgb = model_artifacts['model_xgb']
                explainer = shap.TreeExplainer(model_xgb)
                
                # Trích xuất dữ liệu của quốc gia đó để đưa vào Explainer
                X_country_sample = df_country_year[feature_cols]
                shap_values = explainer(X_country_sample)
                
                # Tính SHAP cho Class 2 (High Risk)
                # shap_values[:, :, class_index]
                shap_class_high = shap_values[0, :, 2]
                
                # Tạo biểu đồ cột ngang SHAP thủ công bằng Matplotlib để hiển thị đẹp trên Streamlit
                shap_df = pd.DataFrame({
                    'Feature': feature_cols,
                    'SHAP Value': shap_class_high.values,
                    'Feature Value': X_country_sample.values[0]
                })
                
                # Lọc ra top 10 features có ảnh hưởng tuyệt đối lớn nhất
                shap_df['Abs SHAP'] = shap_df['SHAP Value'].abs()
                shap_top_10 = shap_df.sort_values(by='Abs SHAP', ascending=False).head(10)
                shap_top_10 = shap_top_10.sort_values(by='SHAP Value', ascending=True) # để vẽ từ dưới lên
                
                # Vẽ biểu đồ matplotlib
                fig_shap, ax_shap = plt.subplots(figsize=(10, 5), facecolor='#0f1116')
                ax_shap.set_facecolor('#1a202c')
                
                colors_shap = ['#f56565' if x > 0 else '#48bb78' for x in shap_top_10['SHAP Value']]
                bars = ax_shap.barh(shap_top_10['Feature'], shap_top_10['SHAP Value'], color=colors_shap)
                
                # Thêm nhãn giá trị thực của feature bên cạnh cột
                for bar, val_feat in zip(bars, shap_top_10['Feature Value']):
                    width = bar.get_width()
                    label_x = width + (0.01 if width >= 0 else -0.05)
                    ax_shap.text(label_x, bar.get_y() + bar.get_height()/2, f"val: {val_feat:.2f}", 
                                 va='center', ha='left' if width >= 0 else 'right', color='white', fontsize=9)
                
                ax_shap.tick_params(colors='white')
                ax_shap.spines['bottom'].set_color('white')
                ax_shap.spines['left'].set_color('white')
                ax_shap.spines['top'].set_visible(False)
                ax_shap.spines['right'].set_visible(False)
                
                plt.title(f"Các chỉ số ảnh hưởng lớn nhất đến dự báo rủi ro của {selected_country}", color='white', fontsize=12)
                st.pyplot(fig_shap)
                
            except Exception as e:
                st.info("Đang nạp công cụ giải thích mô hình...")
                st.write(e)
            
            # 3. Vẽ xu hướng lịch sử các chỉ số kinh tế chính
            st.write("---")
            st.subheader(f"📈 Biểu đồ lịch sử các chỉ số kinh tế của {selected_country}")
            
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                # Biểu đồ GDP & Inflation
                fig_gdp_inf = go.Figure()
                fig_gdp_inf.add_trace(go.Scatter(x=df_country_all['year'], y=df_country_all['gdp_growth'], name="GDP Growth (%)", line=dict(color='cyan', width=2)))
                fig_gdp_inf.add_trace(go.Scatter(x=df_country_all['year'], y=df_country_all['inflation'], name="Inflation (%)", line=dict(color='yellow', width=2)))
                fig_gdp_inf.update_layout(title="Xu hướng GDP Growth và Lạm phát", paper_bgcolor='#1a202c', plot_bgcolor='#1a202c', font=dict(color='white'))
                st.plotly_chart(fig_gdp_inf, use_container_width=True)
                
            with col_chart2:
                # Biểu đồ Unemployment & FDI Inflow
                fig_une_fdi = go.Figure()
                fig_une_fdi.add_trace(go.Scatter(x=df_country_all['year'], y=df_country_all['unemployment'], name="Unemployment (%)", line=dict(color='magenta', width=2)))
                fig_une_fdi.add_trace(go.Scatter(x=df_country_all['year'], y=df_country_all['fdi_inflow'], name="FDI Inflow (% GDP)", line=dict(color='green', width=2)))
                fig_une_fdi.update_layout(title="Xu hướng Thất nghiệp và Dòng vốn FDI", paper_bgcolor='#1a202c', plot_bgcolor='#1a202c', font=dict(color='white'))
                st.plotly_chart(fig_une_fdi, use_container_width=True)
