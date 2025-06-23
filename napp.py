import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from fpdf import FPDF

def safe_text(text):
    try:
        return str(text).encode('latin-1', 'ignore').decode('latin-1')
    except Exception:
        return ''

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

def export_df_to_pdf(df, filename, add_total=False):
    class PDF(FPDF):
        def __init__(self, col_widths, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.col_widths = col_widths
        def header(self):
            self.set_fill_color(220, 220, 220)
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
    pdf.set_draw_color(77, 77, 77)
    fill = False
    line_height = 6.35
    for idx, row in df.iterrows():
        if fill:
            pdf.set_fill_color(240, 240, 240)
        else:
            pdf.set_fill_color(255, 255, 255)
        for i, col in enumerate(df.columns):
            text = safe_text(row[col]) if row[col] is not None else ""
            pdf.cell(pdf.col_widths[i], line_height, text, border=1, align='L', fill=fill)
        pdf.ln(line_height)
        fil
