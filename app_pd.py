import streamlit as st
import pandas as pd
import jdatetime
import io
import datetime
from google.cloud import bigquery

# گرفتن کلید از سکریت
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("کار رو به کاردان بسپار")

# لیست جدول‌ها
table_names = [
    "hspdata",
    "hspdata_02",
    "hspdata_ghor",
    "hspdata_ac",
    "test"
]

selected_table_name = st.selectbox("نام جدول را انتخاب کنید", table_names)

if selected_table_name:
    table_path = f"frsphotspots.HSP.{selected_table_name}"
    query = f"SELECT MAX(UserServiceId) as max_usv FROM `{table_path}`"
    try:
        result = client.query(query).result()
        max_usv = next(result)['max_usv']
    except Exception as e:
        st.error(f"خطا در دریافت حداکثر UserServiceId از بیگ‌کوئری: {e}")
        max_usv = 0

    st.number_input(
        "بزرگ‌ترین UserServiceId ثبت‌شده:",
        min_value=0,
        value=int(max_usv) if max_usv is not None else 0,
        step=1,
        key="readonly_usv",
        disabled=True
    )
    ronumber = max_usv

    # --- آپلود فایل ---
    uploaded_file = st.file_uploader("فایل CSV خود را آپلود کنید", type=["csv"])

    if uploaded_file is not None:
        # خواندن فایل و حذف ستون اضافی اول (Index)
        df_raw = pd.read_csv(uploaded_file)
        if df_raw.columns[0].lower() not in ["cdt", "creatdate"]:
            df_raw = df_raw.iloc[:, 1:]  # حذف ستون اول (index شماره ردیف)

        df = df_raw.copy()

        # تغییر نام ستون‌ها طبق نیاز
        rename_cols = {
            "CDT": "CreatDate",
            "SavingOffUsed": "Package",
        }
        df = df.rename(columns=rename_cols)

        # فقط ستون‌های لازم را نگه‌دار و به همان ترتیب جدول بیگ‌کوئری
        correct_columns = [
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
        df = df[[col for col in correct_columns if col in df.columns]]

        # حذف سطرهایی که UserServiceId کمتر یا مساوی ronumber است
        try:
            df['UserServiceId'] = pd.to_numeric(df['UserServiceId'], errors='coerce')
        except Exception:
            st.error("مشکل در تبدیل ستون UserServiceId به عدد. لطفا فایل را بررسی کنید.")

        df = df[df['UserServiceId'] > ronumber].reset_index(drop=True)

        # تبدیل تاریخ شمسی CDT به میلادی (در صورت نیاز)
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
                    parts = date_str.replace('-', '/').split('/')
                    if len(parts) == 3:
                        gy, gm, gd = map(int, parts)
                        return datetime.date(gy, gm, gd).strftime('%Y-%m-%d')
                return date_str
            except Exception:
                return date_str

        if "CreatDate" in df.columns:
            df["CreatDate"] = df["CreatDate"].astype(str).str.split().str[0]
            df["CreatDate"] = df["CreatDate"].apply(to_gregorian_if_jalali)

        st.success("پردازش انجام شد. داده نهایی:")
        st.dataframe(df.head())

        # --- ارسال به بیگ‌کوئری ---
        if st.button("ارسال به بیگ‌کوئری"):
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
                job = client.load_table_from_dataframe(df, table_path, job_config=job_config)
                job.result()
                added_count = len(df)
                st.success(
                    f"✅ داده‌ها با موفقیت به جدول انتخاب شده بیگ‌کوئری اضافه شدند.\n\n"
                    f"تعداد ردیف افزوده شده: {added_count}"
                )
                if st.button("بازگشت به خانه"):
                    st.experimental_rerun()
            except Exception as e:
                st.error(f"❌ خطا در ارسال داده به بیگ‌کوئری:\n\n{e}")
