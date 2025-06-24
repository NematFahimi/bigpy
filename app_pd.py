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
                # تبدیل نام CDT به CreatDate
                if 'CDT' in df.columns:
                    df = df.rename(columns={'CDT': 'CreatDate'})
                # فقط ستون‌هایی که در جدول بیگ‌کوئری هستند، به ترتیب و نام دقیق
                table_columns = [
                    ('CreatDate', 'date'),
                    ('UserServiceId', 'int'),
                    ('Creator', 'str'),
                    ('ServiceName', 'str'),
                    ('Username', 'str'),
                    ('ServiceStatus', 'str'),
                    ('ServicePrice', 'float'),
                    ('Package', 'float'),
                    ('StartDate', 'str'),
                    ('EndDate', 'str')
                ]
                out_cols = [col for col, _ in table_columns]
                # فقط ستون‌های درست را نگه دار
                df = df[[c for c in out_cols if c in df.columns]]

                # تبدیل تاریخ‌ها و تایپ هر ستون
                if 'CreatDate' in df.columns:
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
                    df['CreatDate'] = df['CreatDate'].astype(str).str.split().str[0]
                    df['CreatDate'] = df['CreatDate'].apply(to_gregorian_if_jalali)

                # تبدیل نوع ستون‌ها مطابق اسکیم جدول
                for col, typ in table_columns:
                    if col in df.columns:
                        if typ == "int":
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        elif typ == "float":
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
                        elif typ == "str":
                            df[col] = df[col].astype(str)
                        elif typ == "date":
                            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date.astype(str)

                st.success("پردازش انجام شد. داده نهایی (فقط ستون‌های صحیح):")
                st.dataframe(df.head())

                if len(df) == 0:
                    st.warning("داده‌ای برای ارسال به جدول وجود ندارد.")
                else:
                    if st.button("ارسال به بیگ‌کوئری"):
                        try:
                            # ترتیب و نام ستون‌ها دقیق و فقط همان ستون‌های جدول باشد
                            df_to_send = df[[col for col, _ in table_columns if col in df.columns]]
                            job_config = bigquery.LoadJobConfig(
                                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                skip_leading_rows=0,
                                source_format=bigquery.SourceFormat.CSV,
                                autodetect=False
                            )
                            job = client.load_table_from_dataframe(df_to_send, table_path, job_config=job_config)
                            job.result()
                            st.session_state.upload_result = ("success", "", len(df_to_send))
                            st.experimental_rerun()
                        except Exception as e:
                            # نمایش خطا و دیتافریم جهت دیباگ
                            st.error("❌ عملیات ارسال داده به جدول موفق نبود.")
                            st.error(f"جزئیات خطا: {e}")
                            st.dataframe(df.head())
                            st.session_state.upload_result = ("fail", str(e), 0)
                            st.experimental_rerun()
