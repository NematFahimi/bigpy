import streamlit as st
import pandas as pd
import jdatetime
import datetime
import numpy as np
from google.cloud import bigquery

st.set_page_config(page_title="BigQuery Uploader", layout="centered")
st.title("📊 بارگذاری داده به BigQuery")

# --- اتصال به BigQuery ---
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

# --- انتخاب جدول ---
table_names = [
    "hspdata",
    "hspdata_02",
    "hspdata_ghor",
    "hspdata_ac",
    "test"
]
selected_table_name = st.selectbox("✅ نام جدول مقصد را انتخاب کنید", table_names)
table_path = f"frsphotspots.HSP.{selected_table_name}"

# --- گرفتن max_usv ---
max_usv = 0
query = f"SELECT MAX(UserServiceId) as max_usv FROM `{table_path}`"
try:
    result = client.query(query).result()
    max_usv = next(result)['max_usv'] or 0
except Exception as e:
    st.warning(f"خطا در دریافت بزرگ‌ترین UserServiceId: {e}")
st.info(f"بزرگترین UserServiceId فعلی: {max_usv}")

# --- آپلود فایل CSV ---
uploaded_file = st.file_uploader("🔽 فایل CSV خام را بارگذاری کنید", type=['csv'])

if uploaded_file:
    df_raw = pd.read_csv(uploaded_file)

    columns_to_drop = [
        'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff',
        'CancelDT', 'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod',
        'InstallmentFirstCash', 'ServiceIsDel'
    ]
    df_clean = df_raw.drop(columns=columns_to_drop, errors='ignore')

    # انتقال ستون تاریخ "CDT" به اول جدول (اگر وجود داشت)
    cols = list(df_clean.columns)
    if "CDT" in cols:
        cols.insert(0, cols.pop(cols.index("CDT")))
        df_clean = df_clean[cols]

    new_columns = [
        "CreatDate",
        "UserServiceId",
        "Creator",
        "ServiceName",
        "Username",
        "ServiceStatus",
        "ServicePrice",
        "Package",
        "StartDate",
        "EndDate"
    ]
    df_clean.columns = new_columns[:len(df_clean.columns)]

    # فقط بخش تاریخ را نگه‌دار
    df_clean['CreatDate'] = df_clean['CreatDate'].astype(str).str.split().str[0]

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

    df_clean['CreatDate'] = df_clean['CreatDate'].apply(to_gregorian_if_jalali)

    # مقادیر ستون‌های ServicePrice و Package کاملاً خالی
    df_clean['ServicePrice'] = np.nan
    df_clean['Package'] = np.nan

    for col in ['Creator', 'ServiceName', 'Username', 'ServiceStatus', 'StartDate', 'EndDate']:
        df_clean[col] = df_clean[col].replace({None: '', 'None': '', 'nan': '', 'NaN': '', np.nan: ''})

    df_clean['UserServiceId'] = pd.to_numeric(df_clean['UserServiceId'], errors='coerce')
    df_clean = df_clean[df_clean['UserServiceId'] > max_usv].reset_index(drop=True)

    st.info(f"تعداد ردیف قابل آپلود: {len(df_clean)}")
    st.dataframe(df_clean)

    if len(df_clean) == 0:
        st.warning("دیتایی برای آپلود وجود ندارد.")
    else:
        if st.button("🚀 ارسال داده‌ها به BigQuery"):
            # تبدیل نوع داده قبل از ارسال
            df_clean['CreatDate'] = pd.to_datetime(df_clean['CreatDate'], errors='coerce').dt.date
            df_clean['UserServiceId'] = pd.to_numeric(df_clean['UserServiceId'], errors='coerce').astype('Int64')
            df_clean['ServicePrice'] = pd.to_numeric(df_clean['ServicePrice'], errors='coerce')
            df_clean['Package'] = pd.to_numeric(df_clean['Package'], errors='coerce')
            for col in ['Creator', 'ServiceName', 'Username', 'ServiceStatus', 'StartDate', 'EndDate']:
                df_clean[col] = df_clean[col].astype(str)

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=0,
                schema=[
                    bigquery.SchemaField("CreatDate", "DATE"),
                    bigquery.SchemaField("UserServiceId", "INTEGER"),
                    bigquery.SchemaField("Creator", "STRING"),
                    bigquery.SchemaField("ServiceName", "STRING"),
                    bigquery.SchemaField("Username", "STRING"),
                    bigquery.SchemaField("ServiceStatus", "STRING"),
                    bigquery.SchemaField("ServicePrice", "FLOAT"),
                    bigquery.SchemaField("Package", "FLOAT"),
                    bigquery.SchemaField("StartDate", "STRING"),
                    bigquery.SchemaField("EndDate", "STRING"),
                ]
            )

            try:
                job = client.load_table_from_dataframe(df_clean, table_path, job_config=job_config)
                job.result()
                st.success(f"✅ آپلود به BigQuery با موفقیت انجام شد. تعداد ردیف‌ها: {len(df_clean)}")
            except Exception as e:
                st.error(f"❌ خطا در ارسال داده به بیگ‌کوئری:\n{e}")
