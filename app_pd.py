import streamlit as st
import pandas as pd
import jdatetime
import datetime
from google.cloud import bigquery

credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("کار رو به کاردان بسپار")

table_names = [
    "hspdata",
    "hspdata_02",
    "hspdata_ghor",
    "hspdata_ac",
    "test"
]

if "upload_result" not in st.session_state:
    st.session_state.upload_result = None

selected_table_name = st.selectbox("نام جدول را انتخاب کنید", table_names)

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

if selected_table_name:
    table_path = f"frsphotspots.HSP.{selected_table_name}"

    # --- دینامیک: دریافت اسکیمای جدول ---
    bq_table = client.get_table(table_path)
    bq_schema = bq_table.schema
    table_columns = [(field.name, field.field_type) for field in bq_schema]

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
    ronumber = max_usv if max_usv is not None else 0

    uploaded_file = st.file_uploader("فایل CSV خود را آپلود کنید", type=["csv"])

    # پیام موفقیت/خطا و دکمه بازگشت
    if st.session_state.upload_result is not None:
        status, msg, rowcount = st.session_state.upload_result
        if status == "success":
            st.success("✅ داده‌ها با موفقیت به جدول انتخاب شده بیگ‌کوئری اضافه شدند.")
            st.info(f"{rowcount:,} سطر جدید به جدول {selected_table_name} اضافه شد.")
        else:
            st.error("❌ عملیات ارسال داده به جدول موفق نبود.")
            st.error(f"جزئیات خطا: {msg}")
        if st.button("بازگشت به خانه"):
            st.session_state.upload_result = None
            st.experimental_rerun()

    elif uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("پیش‌نمایش داده‌های خام:")
        st.dataframe(df.head())

        # فقط اگر ستون UserServiceId هست ادامه بده
        if 'UserServiceId' not in df.columns:
            st.error("ستون UserServiceId در فایل وجود ندارد!")
        else:
            try:
                df['UserServiceId'] = pd.to_numeric(df['UserServiceId'], errors='coerce')
                df = df[df['UserServiceId'].notna()]
                df = df[df['UserServiceId'] > ronumber].reset_index(drop=True)
            except Exception:
                st.error("مشکل در تبدیل ستون UserServiceId به عدد. لطفا فایل را بررسی کنید.")

            if st.button("پردازش داده"):
                # تبدیل نام CDT به CreatDate اگر هست
                if 'CDT' in df.columns and 'CreatDate' not in df.columns:
                    df = df.rename(columns={'CDT': 'CreatDate'})
                # فقط ستون‌های مشترک را نگه دار و طبق ترتیب جدول بچین
                col_names = [col for col, _ in table_columns]
                df_final = pd.DataFrame()
                for col, typ in table_columns:
                    if col in df.columns:
                        # اصلاح نوع داده هر ستون
                        if typ == "DATE":
                            df_final[col] = df[col].astype(str).str.split().str[0].apply(to_gregorian_if_jalali)
                        elif typ in ["STRING"]:
                            df_final[col] = df[col].astype(str)
                        elif typ in ["INTEGER", "INT64"]:
                            df_final[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        elif typ in ["FLOAT", "FLOAT64"]:
                            df_final[col] = pd.to_numeric(df[col], errors='coerce')
                        else:
                            df_final[col] = df[col]
                    else:
                        df_final[col] = None  # اگر ستونی نبود مقدار None

                st.success("پردازش انجام شد. داده نهایی (مطابق جدول بیگ‌کوئری):")
                st.dataframe(df_final.head())

                if len(df_final) == 0:
                    st.warning("داده‌ای برای ارسال به جدول وجود ندارد.")
                else:
                    if st.button("ارسال به بیگ‌کوئری"):
                        try:
                            job_config = bigquery.LoadJobConfig(
                                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                skip_leading_rows=0,
                                source_format=bigquery.SourceFormat.CSV,
                                autodetect=False
                            )
                            job = client.load_table_from_dataframe(df_final, table_path, job_config=job_config)
                            job.result()
                            st.session_state.upload_result = ("success", "", len(df_final))
                            st.experimental_rerun()
                        except Exception as e:
                            st.session_state.upload_result = ("fail", str(e), 0)
                            st.experimental_rerun()
