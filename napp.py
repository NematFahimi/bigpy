import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from fpdf import FPDF

# خواندن اطلاعات کلید از secrets
credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)
table_path = "frsphotspots.HSP.hspdata"

def get_unique_creators():
    query = f"SELECT DISTINCT Creator FROM {table_path} ORDER BY Creator"
    try:
        return [row.Creator for row in client.query(query).result() if row.Creator]
    except Exception as e:
        st.error(f"خطا در دریافت Creatorها: {e}")
        return []

# تابع جدید و بهینه‌شده ساخت PDF
def export_df_to_pdf_optimized(df, filename):
    pdf = FPDF(orientation='P', unit='mm', format='A4')
    margin = 2  # 2mm margin
    pdf.set_auto_page_break(auto=True, margin=margin)
    pdf.set_margins(margin, margin, margin)
    pdf.add_page()

    # A4 Portrait usable width
    usable_width = 210 - 2 * margin
    pdf.set_font("Arial", size=8)

    col_count = len(df.columns)

    # Calculate column widths based on maximum text length in each column
    max_lengths = []
    max_total_length = 0
    for col in df.columns:
        max_len = max([len(str(col))] + [len(str(val)) for val in df[col].head(100)])
        max_lengths.append(max_len)
        max_total_length += max_len

    col_widths = [(max_len / max_total_length) * usable_width for max_len in max_lengths]

    # Draw header row
    def draw_header():
        pdf.set_fill_color(200, 200, 200)  # light gray background for header
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Arial", 'B', 8)  # Bold for header
        for i, col in enumerate(df.columns):
            pdf.cell(col_widths[i], 8, str(col), border=1, align='C', fill=True)
        pdf.ln()

    draw_header()

    # Draw rows with alternating background
    pdf.set_font("Arial", '', 8)  # Normal for rows

    for idx, (_, row) in enumerate(df.iterrows()):
        fill = idx % 2 == 1  # True for odd rows
        if fill:
            pdf.set_fill_color(240, 240, 240)  # 90% white
        else:
            pdf.set_fill_color(255, 255, 255)  # pure white

        for i, col in enumerate(df.columns):
            text = str(row[col]) if row[col] is not None else ""
            pdf.cell(col_widths[i], 8, text, border=1, align='C', fill=True)
        pdf.ln()

        # If near bottom of page, add new page & repeat header
        if pdf.get_y() > 297 - margin - 15:  # page height for A4 Portrait
            pdf.add_page()
            draw_header()

    pdf.output(filename)

st.title("📊 گزارش BigQuery")

creators = get_unique_creators()
selected_creators = st.multiselect("انتخاب Creator", creators)

# فیلتر عددی
with st.expander("فیلتر عددی (UserServiceId)"):
    numeric_option = st.selectbox("نوع شرط", ["بدون فیلتر", "=", ">=", "<=", "بین (BETWEEN)"])
    if numeric_option == "بین (BETWEEN)":
        num_min = st.number_input("حد پایین", step=1, value=0)
        num_max = st.number_input("حد بالا", step=1, value=0)
        numeric_sql = "UserServiceId BETWEEN @usv1 AND @usv2"
        numeric_params = [
            bigquery.ScalarQueryParameter("usv1", "INT64", int(num_min)),
            bigquery.ScalarQueryParameter("usv2", "INT64", int(num_max))
        ]
    elif numeric_option != "بدون فیلتر":
        num_value = st.number_input("عدد", step=1, value=0)
        numeric_sql = f"UserServiceId {numeric_option} @usv1"
        numeric_params = [bigquery.ScalarQueryParameter("usv1", "INT64", int(num_value))]
    else:
        numeric_sql, numeric_params = None, []

# فیلتر تاریخ
with st.expander("فیلتر تاریخ (CreatDate)"):
    date_option = st.selectbox("نوع فیلتر تاریخ", ["بدون فیلتر", "تاریخ خاص (=)", "بین دو تاریخ (BETWEEN)"])
    if date_option == "تاریخ خاص (=)":
        date_value = st.date_input("تاریخ")
        date_sql = "CreatDate = @dt1"
        date_params = [bigquery.ScalarQueryParameter("dt1", "DATE", date_value)]
    elif date_option == "بین دو تاریخ (BETWEEN)":
        date_start = st.date_input("تاریخ شروع")
        date_end = st.date_input("تاریخ پایان")
        date_sql = "CreatDate BETWEEN @dt1 AND @dt2"
        date_params = [
            bigquery.ScalarQueryParameter("dt1", "DATE", date_start),
            bigquery.ScalarQueryParameter("dt2", "DATE", date_end)
        ]
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
            st.write("جدول نتایج:", df)
            export_df_to_pdf_optimized(df, "output.pdf")  # تابع جدید
            with open("output.pdf", "rb") as pdf_file:
                st.download_button(
                    label="📥 دانلود PDF",
                    data=pdf_file,
                    file_name="output.pdf",
                    mime="application/pdf"
                )
        else:
            st.warning("نتیجه‌ای یافت نشد.")
    except Exception as e:
        st.error(f"خطا در اجرای کوئری: {e}")
