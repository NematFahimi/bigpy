import streamlit as st
import pandas as pd
import jdatetime
import datetime
from google.cloud import bigquery

# --- کلید ---
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

# --- تنظیمات صفحه ---
st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("📊 کار رو به کاردان بسپار")

# --- جدول‌ها ---
table_names = [
    "hspdata",
    "hspdata_02",
    "hspdata_ghor",
    "hspdata_ac",
    "test"
]

selected_table_name = st.selectbox("✅ نام جدول را انتخاب کنید", table_names)

if selected_table_name:
    table_path = f"frsphotspots.HSP.{selected_table_name}"
    query = f"SELECT MAX(UserServiceId) as max_usv FROM `{table_path}`"
    try:
        result = client.query(query).result()
        max_usv = next(result)['max_usv']
    except Exception as e:
        st.error(f"خطا در دریافت حداکثر UserServiceId: {e}")
        max_usv = 0

    st.number_input(
        "🔢 بزرگ‌ترین UserServiceId:",
        min_value=0,
        value=int(max_usv) if max_usv is not None else 0,
        step=1,
        key="readonly_usv",
        disabled=True
    )
    ronumber = max_usv

    # --- آپلود فایل ---
    uploaded_file = st.file_uploader("📁 فایل CSV را آپلود کنید", type=["csv"])

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)

        # ✅ حذف ستون رونمبر اضافی اگر هست
        if df.columns[0].lower() not in ["cdt", "creatdate"]:
            df = df.iloc[:, 1:]

        # ✅ تغییر نام ستون‌ها
        rename_map = {
            "CDT": "CreatDate",
            "SavingOffUsed": "Package"
        }
        df = df.rename(columns=rename_map)

        # ✅ ترتیب ستون‌ها دقیق طبق جدول
        target_columns = [
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
        # هر ستونی که نیست → اضافه می‌کنیم خالی
        for col in target_columns:
            if col not in df.columns:
                df[col] = None
        df = df[target_columns]

        st.write("📑 پیش‌نمایش داده‌ها:")
        st.dataframe(df.head())

        try:
            df['UserServiceId'] = pd.to_numeric(df['UserServiceId'], errors='coerce')
        except Exception:
            st.error("🚫 مشکل در تبدیل UserServiceId به عدد")

        if st.button("🔍 پردازش داده"):
            # حذف سطرها با UserServiceId کوچکتر
            df = df[df['UserServiceId'] > ronumber].reset_index(drop=True)

            # فقط بخش تاریخ CreatDate
            df['CreatDate'] = df['CreatDate'].astype(str).str.split().str[0]

            # تابع تبدیل تاریخ
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

            df['CreatDate'] = df['CreatDate'].apply(to_gregorian_if_jalali)

            st.success("✅ پردازش انجام شد:")
            st.dataframe(df.head())

            # --- ارسال به BigQuery ---
            if st.button("🚀 ارسال به بیگ‌کوئری"):
                try:
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        skip_leading_rows=0,
                        source_format=bigquery.SourceFormat.CSV,
                        autodetect=True
                    )
                    job = client.load_table_from_dataframe(df, table_path, job_config=job_config)
                    job.result()
                    st.success(f"✅ داده‌ها ارسال شدند. تعداد ردیف افزوده‌شده: {len(df)}")
                except Exception as e:
                    st.error(f"❌ خطا در ارسال به BigQuery:\n\n{e}")
