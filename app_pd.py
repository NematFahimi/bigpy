import streamlit as st
import pandas as pd
import jdatetime
import io
import datetime
from google.cloud import bigquery

credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("کار رو به کاردان بسپار")

uploaded_file = st.file_uploader("فایل CSV خود را آپلود کنید", type=["csv"])

if uploaded_file is not None:
    # خواندن فایل
    df = pd.read_csv(uploaded_file)
    st.write("پیش‌نمایش داده‌های خام:")
    st.dataframe(df.head())

    # گرفتن رونمبر از کاربر (بررسی نوع داده)
    try:
        df['UserServiceId'] = pd.to_numeric(df['UserServiceId'], errors='coerce')
    except Exception:
        st.error("مشکل در تبدیل ستون UserServiceId به عدد. لطفا فایل را بررسی کنید.")
    
    ronumber = st.number_input("لطفاً رونمبر (UserServiceId) را وارد کنید:", min_value=0, value=0, step=1)

    if st.button("پردازش داده"):
        # حذف ستون‌ها
        columns_to_drop = [
            'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff',
            'CancelDT', 'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod',
            'InstallmentFirstCash', 'ServiceIsDel'
        ]
        df = df.drop(columns=columns_to_drop, errors='ignore')

        # حذف سطرها براساس رونمبر
        df = df[df['UserServiceId'] > ronumber].reset_index(drop=True)
        df['SavingOffUsed'] = None
        df['ServicePrice'] = None

        # انتقال CDT به ابتدای جدول
        cols = list(df.columns)
        if 'CDT' in cols:
            cols.insert(0, cols.pop(cols.index('CDT')))
            df = df[cols]

            # فقط بخش تاریخ را نگه‌دار
            df['CDT'] = df['CDT'].astype(str).str.split().str[0]

            # تابع تبدیل تاریخ با پشتیبانی از همه حالت‌ها
            def to_gregorian_if_jalali(date_str):
                try:
                    if not isinstance(date_str, str):
                        return date_str
                    # اگر تاریخ شمسی است (۱۴xx)
                    if date_str.startswith('14'):
                        parts = date_str.replace('-', '/').split('/')
                        if len(parts) == 3:
                            jy, jm, jd = map(int, parts)
                            gdate = jdatetime.date(jy, jm, jd).togregorian()
                            return gdate.strftime('%Y-%m-%d')
                    # اگر تاریخ میلادی است (۲۰xx)
                    elif date_str.startswith('20'):
                        # همه جداکننده‌ها را به / تبدیل کن
                        parts = date_str.replace('-', '/').split('/')
                        if len(parts) == 3:
                            gy, gm, gd = map(int, parts)
                            return datetime.date(gy, gm, gd).strftime('%Y-%m-%d')
                    # سایر موارد
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
