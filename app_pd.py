import streamlit as st
import pandas as pd
import jdatetime
import io
import datetime
from google.cloud import bigquery

# اتصال به BigQuery
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

# پیکربندی صفحه
st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("کار رو به کاردان بسپار")

# -- بخش ۱: انتخاب جدول --
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
    ronumber = max_usv

    # -- بخش ۲: آپلود فایل و پردازش --
    uploaded_file = st.file_uploader("فایل CSV خود را آپلود کنید", type=["csv"])

    if uploaded_file is not None:
        # خواندن فایل
        df = pd.read_csv(uploaded_file)
        st.write("پیش‌نمایش داده‌های خام:")
        st.dataframe(df.head())

        # تبدیل ستون UserServiceId به عدد
        try:
            df['UserServiceId'] = pd.to_numeric(df['UserServiceId'], errors='coerce')
        except Exception:
            st.error("مشکل در تبدیل ستون UserServiceId به عدد. لطفا فایل را بررسی کنید.")

        if st.button("پردازش داده"):
            # حذف ستون‌های اضافی
            columns_to_drop = [
                'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff',
                'CancelDT', 'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod',
                'InstallmentFirstCash', 'ServiceIsDel'
            ]
            df = df.drop(columns=columns_to_drop, errors='ignore')

            # حذف سطرهای تکراری براساس UserServiceId
            df = df[df['UserServiceId'] > ronumber].reset_index(drop=True)
            df['SavingOffUsed'] = None
            df['ServicePrice'] = None

            # انتقال CDT به ابتدا
            cols = list(df.columns)
            if 'CDT' in cols:
                cols.insert(0, cols.pop(cols.index('CDT')))
                df = df[cols]

                # فقط تاریخ را نگه‌دار و تبدیل تاریخ
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

            # ✅ تغییر نام ستون‌ها طبق درخواست
            df.columns = [
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

            # ✅ حذف index اضافه برای نمایش و آپلود
            df.reset_index(drop=True, inplace=True)

            st.success("پردازش انجام شد. داده نهایی:")
            st.dataframe(df.head())

            # --- دکمه ارسال به BigQuery ---
            if st.button("ارسال به بیگ‌کوئری"):
                try:
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        skip_leading_rows=0,
                        source_format=bigquery.SourceFormat.CSV,
                        autodetect=True
                    )
                    job = client.load_table_from_dataframe(df, table_path, job_config=job_config)
                    job.result()
                    st.success("✅ داده‌ها با موفقیت به جدول انتخاب‌شده بیگ‌کوئری اضافه شدند.")
                except Exception as e:
                    st.error(f"❌ خطا در ارسال داده به بیگ‌کوئری: {e}")
