import streamlit as st
import pandas as pd

# تنظیمات صفحه و عنوان
st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("📊 کار رو به کاردان بسپار")

# آپلود فایل CSV
uploaded_file = st.file_uploader("📁 فایل CSV خود را آپلود کنید", type=["csv"])

# اگر فایل آپلود شد، پیش‌نمایش داده‌ها را نشان بده
if uploaded_file is not None:
    df_raw = pd.read_csv(uploaded_file)
    st.write("🗂️ پیش‌نمایش داده‌های خام:")
    st.dataframe(df_raw)
