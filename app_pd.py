import streamlit as st
import pandas as pd
import jdatetime
import datetime
import numpy as np
from google.cloud import bigquery
import io

st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("📊 کار رو به کاردان بسپار")

# اتصال به BigQuery
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

table_names = [
    "hspdata",
    "hspdata_02",
    "hspdata_ghor",
    "hspdata_ac",
    "test"
]

selected_table_name = st.selectbox("✅ نام جدول را انتخاب کنید", table_names)

max_usv = 0
if selected_table_name:
    table_path = f"frsphotspots.HSP.{selected_table_name}"
    query = f"SELECT MAX(UserServiceId) as max_usv FROM `{table_path}`"
    try:
        result = client.query(query).result()
        max_usv = next(result)['max_usv'] or 0
    except Exception as e:
        st.error(f"خطا در دریافت بزرگ‌ترین UserServiceId: {e}")
        max_usv = 0

    st.info(f"جدول تا شماره **{max_usv}** آپدیت است.")

uploaded_file = st.file_uploader("📁 فایل CSV خود را آپلود کنید", type=["csv"])

if uploaded_file is not None:
    df_raw = pd.read_csv(uploaded_file)
    st.write("🗂️ پیش‌نمایش داده‌های خام (۱۰ سطر اول):")
    st.dataframe(df_raw.head(10))

    if st.button("🧹 Clean Data"):
        columns_to_drop = [
            'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff',
            'CancelDT', 'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod',
            'InstallmentFirstCash', 'ServiceIsDel'
        ]
        df_clean = df_raw.drop(columns=columns_to_drop, errors='ignore')

        # انتقال ستون تاریخ "CDT" به ابتدای جدول (اگر وجود داشت)
        cols = list(df_clean.columns)
        if "CDT" in cols:
            cols.insert(0, cols.pop(cols.index("CDT")))
            df_clean = df_clean[cols]

        # تغییر نام ستون‌ها طبق اسکیم
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

        # تابع تبدیل تاریخ به میلادی
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

        # مقادیر ستون‌های ServicePrice و Package کاملاً خالی (بدون هیچ مقدار NaN یا None یا 'nan')
        df_clean['ServicePrice'] = np.nan
        df_clean['Package'] = np.nan

        # اطمینان از خالی بودن کامل رشته‌ای‌ها (در صورت نیاز)
        for col in ['Creator', 'ServiceName', 'Username', 'ServiceStatus', 'StartDate', 'EndDate']:
            df_clean[col] = df_clean[col].replace({None: '', 'None': '', 'nan': '', 'NaN': '', np.nan: ''})

        # حذف ردیف‌هایی که UserServiceId آنها <= max_usv باشد
        try:
            df_clean['UserServiceId'] = pd.to_numeric(df_clean['UserServiceId'], errors='coerce')
            df_clean = df_clean[df_clean['UserServiceId'] > max_usv].reset_index(drop=True)
        except Exception:
            st.warning("⚠️ خطا در تبدیل یا فیلتر کردن UserServiceId. لطفاً صحت داده‌ها را بررسی کنید.")

        st.success("✅ پاکسازی کامل شد! ۱۰ سطر اول داده نهایی:")
        st.dataframe(df_clean.head(10))

        # --- دکمه دانلود خروجی به CSV برای کاربر ---
        if not df_clean.empty:
            csv_buffer = io.StringIO()
            df_clean.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            st.download_button(
                label="⬇️ دانلود خروجی CSV",
                data=csv_buffer.getvalue(),
                file_name="cleaned_output.csv",
                mime="text/csv"
            )
