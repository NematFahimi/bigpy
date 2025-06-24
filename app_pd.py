import streamlit as st
import pandas as pd
import jdatetime
import io
import datetime
from google.cloud import bigquery

# ساخت کلاینت بیگ‌کوئری
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("کار رو به کاردان بسپار")

# 1. گرفتن بزرگ‌ترین UserServiceId از بیگ‌کوئری
table_path = "frsphotspots.HSP.hspdata"
query = f"SELECT MAX(UserServiceId) as max_usv FROM `{table_path}`"
try:
    result = client.query(query).result()
    max_usv = next(result)['max_usv']
except Exception as e:
    st.error(f"خطا در دریافت حداکثر UserServiceId از بیگ‌کوئری: {e}")
    max_usv = 0

# نمایش readonly مقدار max_usv به کاربر
st.markdown("**بزرگ‌ترین UserServiceId ثبت‌شده:**")
st.number_input(
    "Max UserServiceId",
    min_value=0,
    value=int(max_usv) if max_usv is not None else 0,
    step=1,
    format="%d",
    key="readonly_usv",
    disabled=True
)

uploaded_file = st.file_uploader("فایل CSV خود را آپلود کنید", type=["csv"])

if uploaded_file is not None:
    # خواندن فایل
    df = pd.read_csv(uploaded_file)
    st.write("پیش‌نمایش داده‌های خام:")
    st.dataframe(df.head())

    # گرفتن رونمبر از ستون (بررسی نوع داده)
    try:
        df['UserServiceId'] = pd.to_numeric(df['UserServiceId'], errors='coerce')
    except Exception:
        st.error("مشکل در تبدیل ستون UserServiceId به عدد. لطفا فایل را بررسی کنید.")

    if st.button("پردازش داده"):
        # حذف ستون‌ها
        columns_to_drop = [
            'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff',
            'CancelDT', 'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod',
            'InstallmentFirstCash', 'ServiceIsDel'
        ]
        df = df.drop(columns=columns_to_drop, errors='ignore')

        # حذف سطرها براساس max_usv که از بیگ‌کوئری آمد
        df = df[df['UserServiceId'] > max_usv].reset_index(drop=True)
        df['SavingOffUsed'] = None
        df['ServicePrice'] = None

        # انتقال CDT به ابتدای جدول و تبدیل تاریخ‌ها
        cols = list(df.columns)
        if 'CDT' in cols:
            cols.insert(0, cols.pop(cols.index('CDT')))
            df = df[cols]

            df['CDT'] = df['CDT'].astype(str).str.split().str[0]

            def to_gregorian_if_jalali(date_str):
                try:
                    if not isinstance(date_str, str):
                        return date_str
                    if date_str.startswith('14'):
                        parts = date_str.replace('-', '/').split('/')
                        if len(parts) == 3:
                            jy, jm, jd = map(int, parts)
                            gdate = jdatetime.date(jy, jm, jd).togregorian()
                            return gdate.strftime('%Y-%m-%d')
                    elif date_str.startswith('20'):
                        parts = date_str.replace('-', '/').split('/')
                        if len(parts) == 3:
                            gy, gm, gd = map(int, parts)
                            return datetime.date(gy, gm, gd).strftime('%Y-%m-%d')
                    return date_str
                except Exception:
                    return date_str

            df['CDT'] = df['CDT'].apply(to_gregorian_if_jalali)

        st.success("پردازش انجام شد. داده نهایی:")
        st.dataframe(df.head())

        # ساختن فایل خروجی برای دانلود
        towrite = io.BytesIO()
        df.to_csv(towrite, index=False, encoding='utf-8-sig')
        towrite.seek(0)
        st.download_button(
            label="دانلود فایل خروجی CSV",
            data=towrite,
            file_name="ServiceReport_cleaned.csv",
            mime="text/csv"
        )
