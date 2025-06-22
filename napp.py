import streamlit as st
from google.cloud import bigquery
from datetime import datetime
from fpdf import FPDF
import pandas as pd
import os

# بارگذاری کلید از Streamlit Secrets
credentials_info = st.secrets["gcp_service_account"]
client = bigquery.Client.from_service_account_info(credentials_info)

table_path = "frsphotspots.HSP.hspdata"

@st.cache_data
def get_unique_creators():
    query = f"SELECT DISTINCT Creator FROM {table_path} ORDER BY Creator"
    try:
        return [row.Creator for row in client.query(query).result() if row.Creator]
    except Exception as e:
        st.error(f"خطا در دریافت Creatorها: {e}")
        return []

def export_df_to_pdf(df, filename):
    pdf = FPDF(orientation='L', unit='mm', format='A4')
    margin = 0.5
    pdf.set_auto_page_break(auto=True, margin=margin)
    pdf.set_margins(margin, margin, margin)
    pdf.add_page()

    usable_width = 297 - 2 * margin
    col_count = len(df.columns)

    font_size = max(5, min(9, int(usable_width / (col_count * 4))))
    pdf.set_font("Arial", size=font_size)

    col_width = usable_width / col_count if col_count > 0 else 40

    for col in df.columns:
        pdf.cell(col_width, 8, str(col), border=1, align='C')
    pdf.ln()

    for _, row in df.iterrows():
        for col in df.columns:
            cell_text = str(row[col]) if row[col] is not None else ""
            max_char = int(col_width * (font_size / 2.5))
            display_text = cell_text[:max_char]
            pdf.cell(col_width, 8, display_text, border=1, align='C')
        pdf.ln()

    pdf.output(filename)

st.title("📊 برنامه گزارش از BigQuery")

creators = get_unique_creators()
selected_creators = st.multiselect("Creator را انتخاب کنید", creators, default=[])

with st.expander("🔢 فیلتر عددی (UserServiceId)"):
    numeric_option = st.selectbox("انتخاب شرط", ["هیچ‌کدام", "=", ">=", "<=", "BETWEEN"])
    if numeric_option == "BETWEEN":
        num_min = st.number_input("حد پایین", step=1)
        num_max = st.number_input("حد بالا", step=1)
        numeric_sql = "UserServiceId BETWEEN @usv1 AND @usv2"
        numeric_params = [bigquery.ScalarQueryParameter("usv1", "INT64", num_min),
                          bigquery.ScalarQueryParameter("usv2", "INT64", num_max)]
    elif numeric_option != "هیچ‌کدام":
        num_value = st.number_input("عدد", step=1)
        numeric_sql = f"UserServiceId {numeric_option} @usv1"
        numeric_params = [bigquery.ScalarQueryParameter("usv1", "INT64", num_value)]
    else:
        numeric_sql, numeric_params = None, []

with st.expander("📅 فیلتر تاریخ (CreatDate)"):
    date_option = st.selectbox("نوع شرط تاریخ", ["هیچ‌کدام", "تاریخ خاص", "بین دو تاریخ"])
    if date_option == "تاریخ خاص":
        date_value = st.date_input("تاریخ")
        date_sql = "CreatDate = @dt1"
        date_params = [bigquery.ScalarQueryParameter("dt1", "DATE", date_value)]
    elif date_option == "بین دو تاریخ":
        date_start = st.date_input("تاریخ شروع")
        date_end = st.date_input("تاریخ پایان")
        date_sql = "CreatDate BETWEEN @dt1 AND @dt2"
        date_params = [bigquery.ScalarQueryParameter("dt1", "DATE", date_start),
                       bigquery.ScalarQueryParameter("dt2", "DATE", date_end)]
    else:
        date_sql, date_params = None, []

if st.button("اجرای کوئری"):
    conditions, params = [], []

    if selected_creators:
        conditions.append("Creator IN UNNEST(@creator_list)")
        params.append(bigquery.ArrayQueryParameter("creator_list", "STRING", selected_creators))
    if numeric_sql:
        conditions.append(numeric_sql)
        params += numeric_params
    if date_sql:
        conditions.append(date_sql)
        params += date_params

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"SELECT * FROM {table_path} {where_clause}"

    try:
        results = client.query(query, bigquery.QueryJobConfig(query_parameters=params)).result()
        rows = [dict(row) for row in results]

        if rows:
            df = pd.DataFrame(rows)
            st.write("نتایج کوئری:", df.head(20))
            export_df_to_pdf(df, "output.pdf")
            with open("output.pdf", "rb") as pdf_file:
                st.download_button(label="📥 دانلود PDF",
                                   data=pdf_file,
                                   file_name="output.pdf",
                                   mime="application/pdf")
        else:
            st.warning("نتیجه‌ای یافت نشد.")
    except Exception as e:
        st.error(f"خطا در اجرای کوئری: {e}")
