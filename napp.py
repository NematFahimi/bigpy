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
                self.cell(self.col_widths[i], 8, str(col), border=1, align='C', fill=True)
            self.ln()

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
        col_len = pdf_tmp.get_string_width(str(col)) + 2
        max_val = max([pdf_tmp.get_string_width(str(val)) for val in df[col].astype(str)] + [col_len])
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
    font_size = 8
    line_height = font_size * 0.5 + 4  # حدودی و قابل تنظیم

    for idx, row in df.iterrows():
        # تعداد خطوط مورد نیاز هر سلول
        cell_lines = []
        for i, col in enumerate(df.columns):
            text = str(row[col]) if row[col] is not None else ""
            cw = pdf.col_widths[i]
            try:
                pdf.set_font("Arial", size=font_size)
            except:
                pdf.set_font("helvetica", size=font_size)
            # فقط تقسیم متن به خطوط (بدون چاپ)
            text_lines = pdf.multi_cell(cw, line_height, text, border=0, align='L', split_only=True)
            cell_lines.append(len(text_lines))
        max_lines = max(cell_lines)
        max_height = max_lines * line_height

        # رنگ پس‌زمینه
        if fill:
            pdf.set_fill_color(240, 240, 240)
        else:
            pdf.set_fill_color(255, 255, 255)

        x_start = pdf.get_x()
        y_start = pdf.get_y()

        # چاپ سلول‌ها با ارتفاع یکنواخت
        for i, col in enumerate(df.columns):
            text = str(row[col]) if row[col] is not None else ""
            cw = pdf.col_widths[i]
            x = pdf.get_x()
            y = pdf.get_y()
            # مختصات اولیه
            # multi_cell چاپ با حداکثر ارتفاع ردیف (در انتها به سطر بعد نمی‌رود)
            pdf.multi_cell(cw, line_height, text, border=1, align='L', fill=fill, max_line_height=pdf.font_size_pt)
            # رفتن به جایگاه بعدی سلول
            pdf.set_xy(x + cw, y)
        # حرکت به سطر بعد
        pdf.set_xy(x_start, y_start + max_height)

        fill = not fill

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
    elif numeric_option != "بدون فی_
