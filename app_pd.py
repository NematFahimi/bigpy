import streamlit as st
import pandas as pd

# تنظیمات صفحه و عنوان
st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("📊 کار رو به کاردان بسپار")

# آپلود فایل CSV
uploaded_file = st.file_uploader("📁 فایل CSV خود را آپلود کنید", type=["csv"])

# اگر فایل آپلود شد، فقط ۱۰ سطر اول را نمایش بده
if uploaded_file is not None:
    df_raw = pd.read_csv(uploaded_file)
    st.write("🗂️ پیش‌نمایش داده‌های خام (۱۰ سطر اول):")
    st.dataframe(df_raw.head(10))

    # دکمه Clean Data
    if st.button("🧹 Clean Data"):
        st.info("هنوز هیچ عملکردی برای پاکسازی داده تعریف نشده است.")
