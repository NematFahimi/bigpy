import streamlit as st
from google.cloud import bigquery

st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("📊 کار رو به کاردان بسپار")

# اتصال به BigQuery
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)

# ---- بخش انتخاب جدول ----
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
        max_usv = next(result)['max_usv'] or 0
    except Exception as e:
        st.error(f"خطا در دریافت بزرگ‌ترین UserServiceId: {e}")
        max_usv = 0

    st.info(f"جدول تا شماره **{max_usv}** آپدیت است.")
