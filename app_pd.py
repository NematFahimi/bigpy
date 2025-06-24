import streamlit as st
import pandas as pd
import jdatetime
import io
import datetime
from google.cloud import bigquery
import traceback

# گرفتن کلید از سکریت (فرمت TOML)
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("کار رو به کاردان بسپار")

# -- بخش ۱: انتخاب جدول (فقط نام جدول) --
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

    uploaded_file = st.file_uploader("فایل CSV خود را آپلود کنید", type=["csv"])

    # --- وضعیت مرحله پردازش/ارسال را مدیریت کن ---
    if 'processed_df' not in st.session_state:
        st.session_state.processed_df = None
    if 'show_upload_btn' not in st.session_state:
        st.session_state.show_upload_btn = False
    if 'upload_result' not in st.session_state:
        st.session_state.upload_result = None

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("پیش‌نمایش داده‌های خام:")
        st.dataframe(df.head())

        try:
            df['UserServiceId'] = pd.to_numeric(df['UserServiceId'], errors='coerce')
        except Exception:
            st.error("مشکل در تبدیل ستون UserServiceId به عدد. لطفا فایل را بررسی کنید.")

        if st.button("پردازش داده"):
            columns_to_drop = [
                'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff',
                'CancelDT', 'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod',
                'InstallmentFirstCash', 'ServiceIsDel'
            ]
            df = df.drop(columns=columns_to_drop, errors='ignore')
            df = df[df['UserServiceId'] > ronumber].reset_index(drop=True)
            df['SavingOffUsed'] = None
            df['ServicePrice'] = None

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
            st.session_state.processed_df = df
            st.session_state.show_upload_btn = True
            st.session_state.upload_result = None

    # فقط وقتی پردازش انجام شده دکمه ارسال به بیگ‌کوئری را نشان بده
    if st.session_state.show_upload_btn and st.session_state.processed_df is not None:
        if st.button("ارسال به بیگ‌کوئری"):
            try:
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    skip_leading_rows=0,
                    source_format=bigquery.SourceFormat.CSV,
                    autodetect=True
                )
                job = client.load_table_from_dataframe(st.session_state.processed_df, table_path, job_config=job_config)
                job.result()

                added_count = len(st.session_state.processed_df)
                st.session_state.upload_result = ("success", added_count)
                st.session_state.show_upload_btn = False
            except Exception as e:
                st.session_state.upload_result = ("error", f"{e}\n{traceback.format_exc()}")
                st.session_state.show_upload_btn = False

    # نمایش نتیجه عملیات ارسال (موفق یا خطا)
    if st.session_state.upload_result is not None:
        if st.session_state.upload_result[0] == "success":
            st.success(f"✅ داده‌ها با موفقیت به جدول انتخاب شده بیگ‌کوئری اضافه شدند.\n"
                       f"تعداد ردیف افزوده شده: {st.session_state.upload_result[1]}")
            if st.button("بازگشت به خانه"):
                st.session_state.processed_df = None
                st.session_state.upload_result = None
                st.experimental_rerun()
        elif st.session_state.upload_result[0] == "error":
            st.error(f"❌ خطا در ارسال داده به بیگ‌کوئری:\n\n{st.session_state.upload_result[1]}")
            if st.button("بازگشت به خانه"):
                st.session_state.processed_df = None
                st.session_state.upload_result = None
                st.experimental_rerun()
