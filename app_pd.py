import streamlit as st
import pandas as pd
import jdatetime
import datetime
from google.cloud import bigquery

# ====== تنظیم کلید سرویس ======
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

# ====== تنظیم ظاهر ======
st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("📊 کار رو به کاردان بسپار")

# ====== لیست جدول‌ها ======
table_names = [
    "hspdata",
    "hspdata_02",
    "hspdata_ghor",
    "hspdata_ac",
    "test"
]

selected_table_name = st.selectbox("✅ نام جدول را انتخاب کنید", table_names)

# ====== نام دقیق فیلدها طبق جدول BigQuery ======
expected_columns = [
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

# ====== شروع پردازش ======
if selected_table_name:
    table_path = f"frsphotspots.HSP.{selected_table_name}"
    query = f"SELECT MAX(UserServiceId) as max_usv FROM `{table_path}`"
    try:
        result = client.query(query).result()
        max_usv = next(result)['max_usv'] or 0
    except Exception as e:
        st.error(f"خطا در دریافت بزرگ‌ترین UserServiceId از بیگ‌کوئری: {e}")
        max_usv = 0

    st.number_input(
        "🔢 بزرگ‌ترین UserServiceId موجود:",
        min_value=0,
        value=int(max_usv),
        step=1,
        key="readonly_usv",
        disabled=True
    )
    ronumber = max_usv

    # ====== آپلود فایل ======
    uploaded_file = st.file_uploader("📁 فایل CSV خود را آپلود کنید", type=["csv"])

    if uploaded_file is not None:
        df_raw = pd.read_csv(uploaded_file)

        # ====== حذف ستون ایندکس اگر به‌صورت ستون آمده ======
        if df_raw.columns[0].lower() not in ["cdt", "creatdate"]:
            df_raw = df_raw.iloc[:, 1:]

        # ====== تغییر نام بعضی ستون‌ها ======
        rename_map = {
            "CDT": "CreatDate",
            "SavingOffUsed": "Package"
        }
        df = df_raw.rename(columns=rename_map).copy()

        # ====== انتخاب ستون‌های مورد انتظار ======
        clean_df = pd.DataFrame()
        for col in expected_columns:
            if col in df.columns:
                clean_df[col] = df[col]
            else:
                clean_df[col] = None

        # ====== تبدیل CreatDate به میلادی ======
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
            except:
                return date_str

        clean_df['CreatDate'] = clean_df['CreatDate'].astype(str).str.split().str[0]
        clean_df['CreatDate'] = clean_df['CreatDate'].apply(to_gregorian_if_jalali)

        # ====== تبدیل UserServiceId به عدد ======
        clean_df['UserServiceId'] = pd.to_numeric(clean_df['UserServiceId'], errors='coerce')

        # ====== حذف ردیف‌های قدیمی ======
        clean_df = clean_df[clean_df['UserServiceId'] > ronumber].reset_index(drop=True)

        # ====== نمایش پیش‌نمایش ======
        st.success("✅ پردازش انجام شد. داده نهایی:")
        st.dataframe(clean_df)

        # ====== ارسال به BigQuery ======
        if st.button("🚀 ارسال به بیگ‌کوئری"):
            try:
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
                    ],
                )
                job = client.load_table_from_dataframe(clean_df, table_path, job_config=job_config)
                job.result()

                st.success(f"✅ ارسال موفق بود! تعداد ردیف افزوده‌شده: {len(clean_df)}")

                if st.button("🏠 بازگشت به خانه"):
                    st.experimental_rerun()

            except Exception as e:
                st.error(f"❌ خطا در ارسال به بیگ‌کوئری:\n\n{e}")
