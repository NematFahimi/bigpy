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

# -- بخش ۱: انتخاب جدول (فقط نام جدول) --
table_names = [
    "hspdata",
    "hspdata_02",
    "hspdata_ghor",
    "hspdata_ac",
    "test"  # جدول تست اضافه شد
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
    ronumber = max_usv if max_usv is not None else 0

    # -- بخش ۲: آپلود فایل و پردازش فقط بعد از انتخاب جدول --
    uploaded_file = st.file_uploader("فایل CSV خود را آپلود کنید", type=["csv"])

    if uploaded_file is not None:
        # خواندن فایل
        df = pd.read_csv(uploaded_file)
        st.write("پیش‌نمایش داده‌های خام:")
        st.dataframe(df.head())

        # تلاش برای تبدیل ستون UserServiceId به عدد و حذف نال‌ها
        if 'UserServiceId' not in df.columns:
            st.error("ستون UserServiceId در فایل وجود ندارد!")
        else:
            try:
                df['UserServiceId'] = pd.to_numeric(df['UserServiceId'], errors='coerce')
                n_before = len(df)
                # حذف ردیف‌هایی که UserServiceId نال یا کوچکتر یا مساوی ronumber هستند
                df = df[df['UserServiceId'].notna()]
                df = df[df['UserServiceId'] > ronumber].reset_index(drop=True)
                n_after = len(df)
                if n_after == 0:
                    st.warning("هیچ سطر جدیدی نسبت به جدول انتخابی وجود ندارد.")
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

                # --- بخش جدید: ارسال به بیگ‌کوئری ---
                if st.button("ارسال به بیگ‌کوئری"):
                    if len(df) == 0:
                        st.warning("داده‌ای برای ارسال به جدول وجود ندارد.")
                    else:
                        try:
                            # آپلود به جدول انتخابی، حالت append
                            job_config = bigquery.LoadJobConfig(
                                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                skip_leading_rows=0,  # چون دیتافریم است، ردیف عنوان لازم نیست
                                source_format=bigquery.SourceFormat.CSV,
                                autodetect=True      # ساختار جدول را از دیتافریم بگیرد
                            )
                            job = client.load_table_from_dataframe(df, table_path, job_config=job_config)
                            job.result()  # منتظر بماند تا آپلود کامل شود
                            st.success("✅ داده‌ها با موفقیت به جدول انتخاب شده بیگ‌کوئری اضافه شدند.")
                        except Exception as e:
                            st.error(f"❌ خطا در ارسال داده به بیگ‌کوئری: {e}")
