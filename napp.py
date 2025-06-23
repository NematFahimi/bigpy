import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from fpdf import FPDF

# تابع کمکی برای حذف کاراکترهای غیرلاتین
def safe_text(text):
    try:
        return str(text).encode('latin-1', 'ignore').decode('latin-1')
    except Exception:
        return ''

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

def export_df_to_pdf(df, filename):
    class PDF(FPDF):
        def __init__(self, col_widths, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.col_widths = col_widths

        def header(self):
            self.set_fill_color(220, 220, 220)  # هدر خاکستری
            self.set_text_color(0)
            try:
                self.set_font("Arial", size=8)
            except:
                self.set_font("helvetica", size=8)
            for i, col in enumerate(df.columns):
                pdf_text = safe_text(col)
                self.cell(self.col_widths[i], 6.35, pdf_text, border=1, align='C', fill=True)
            self.ln(6.35)

    if df.empty:
        return

    margin = 2
    usable_width = 210 - 2 * margin  # Portrait A4

    pdf_tmp = FPDF()
    try:
        pdf_tmp.set_font("Arial", size=8)
    except:
        pdf_tmp.set_font("helvetica", size=8)
    max_lens = []
    for col in df.columns:
        col_len = pdf_tmp.get_string_width(safe_text(col)) + 2
        max_val = max([pdf_tmp.get_string_width(safe_text(val)) for val in df[col].astype(str)] + [col_len])
        max_lens.append(max_val)
    total_width = sum(max_lens)
    col_widths = [w * usable_width / total_width for w in max_lens]

    pdf = PDF(col_widths, orientation='P', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=margin)
    pdf.set_margins(margin, margin, margin)
    pdf.add_page()
    try:
        pdf.set_font("Arial", size=8)
    except:
        pdf.set_font("helvetica", size=8)
    pdf.set_draw_color(77, 77, 77)  # 30% سیاه

    fill = False
    line_height = 6.35  # معادل 0.25 اینچ

    for idx, row in df.iterrows():
        if fill:
            pdf.set_fill_color(240, 240, 240)
        else:
            pdf.set_fill_color(255, 255, 255)
        for i, col in enumerate(df.columns):
            text = safe_text(row[col]) if row[col] is not None else ""
            pdf.cell(pdf.col_widths[i], line_height, text, border=1, align='L', fill=fill)
        pdf.ln(line_height)
        fill = not fill

    # -------- افزودن سطر مجموع در انتهای جدول ---------
    pdf.set_fill_color(200, 220, 255)  # رنگ پس‌زمینه مجموع متفاوت
    try:
        pdf.set_font("Arial", size=8)
    except:
        pdf.set_font("helvetica", size=8)

    sum_row = []
    package_sum = df['package'].astype(float).sum() if 'package' in df.columns else ''
    usid_count = df['UserServiceId'].count() if 'UserServiceId' in df.columns else ''
    first = True
    for col in df.columns:
        if col == 'package':
            sum_row.append(str(package_sum))
        elif col == 'UserServiceId':
            sum_row.append(str(usid_count))
        elif first:
            sum_row.append(safe_text('مجموع'))
            first = False
        else:
            sum_row.append('')

    for i, text in enumerate(sum_row):
        pdf.cell(pdf.col_widths[i], line_height, text, border=1, align='C', fill=True)
    pdf.ln(line_height)
    # -------- پایان سطر مجموع ---------

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
            export_df_to_pdf(df, "output.pdf")
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
