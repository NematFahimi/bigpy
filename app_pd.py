import streamlit as st
import pandas as pd
import numpy as np
import jdatetime
import re

st.set_page_config(page_title="پردازش فایل CSV خدمات کاربران", layout="wide")

st.title("🧾 برنامه پردازش گزارش خدمات کاربران")

uploaded_file = st.file_uploader("📤 فایل CSV را آپلود کنید", type=["csv"])

# تشخیص تاریخ شمسی
def is_jalali_date(date_str):
    try:
        if not isinstance(date_str, str):
            return False
        return re.search(r"\d{4}/\d{2}/\d{2}", date_str) is not None
    except:
        return False

# تبدیل تاریخ شمسی به میلادی
def jalali_to_gregorian(date_str):
    try:
        match = re.search(r"(\d{4}/\d{2}/\d{2})", str(date_str))
        if not match:
            return None
        y, m, d = map(int, match.group(1).split("/"))
        g_date = jdatetime.date(y, m, d).togregorian()
        return g_date.strftime('%Y-%m-%d')
    except:
        return None

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    st.success("✅ فایل با
