import pandas as pd
from google.cloud import bigquery
from fpdf import FPDF

def safe_text(text):
    try:
        return str(text).encode('latin-1', 'ignore').decode('latin-1')
    except Exception:
        return ''

# ---- تنظیمات کلید سرویسی خود را اینجا بده ----
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"path_to_your_gcp_service_key.json"

client = bigquery.Client()
tables_priority = ["hspdata", "hspdata_02", "hspdata_ghor"]

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
    usable_width = 210 - 2 * margin
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
    pdf.set_draw_color(51, 51, 51)
    fill = False
    line_height = 6.35
    for idx, row in df.iterrows():
        is_total_row = (
            (str(row[0]).strip().endswith("Total")) or
            (str(row[0]).strip().lower() == "grand total")
        )
        if is_total_row:
            pdf.set_fill_color(200, 210, 210)
        elif fill:
            pdf.set_fill_color(240, 240, 240)
        else:
            pdf.set_fill_color(255, 255, 255)
        for i, col in enumerate(df.columns):
            text = safe_text(row[col]) if row[col] is not None else ""
            pdf.cell(pdf.col_widths[i], line_height, text, border=1, align='L', fill=True)
        pdf.ln(line_height)
        fill = not fill
    pdf.output(filename)
    print(f"PDF ذخیره شد: {filename}")

def find_creator_data(creator, query_base, params, tables_priority):
    for table_name in tables_priority:
        query = query_base.format(table_path=f"frsphotspots.HSP.{table_name}")
        try:
            res = client.query(query, bigquery.QueryJobConfig(query_parameters=params)).result()
            rows = [dict(row) for row in res]
            print(f"جدول: {table_name}, Creator: {creator}, تعداد رکورد: {len(rows)}")
            if rows:
                return pd.DataFrame(rows), table_name
        except Exception as e:
            print(f"خطا در جدول {table_name}: {e}")
            continue
    print(f"{creator} در هیچ جدول یافت نشد.")
    return pd.DataFrame(), None

# ======== ورودی از کاربر ========

creators_raw = input("نام Creatorها را با کاما وارد کن (مثال: Ali,Zahra,Mohsen): ")
selected_creators = [c.strip() for c in creators_raw.split(',') if c.strip()]

filter_serial = input("آیا می‌خواهی سریال فیلتر شود؟ (y/n): ").strip().lower() == 'y'
numeric_sql, numeric_params = None, []
if filter_serial:
    op = input("نوع فیلتر (مثلاً =, >=, <=, BETWEEN): ")
    if op == "BETWEEN":
        num_min = int(input("حد پایین: "))
        num_max = int(input("حد بالا: "))
        numeric_sql = "UserServiceId BETWEEN @usv1 AND @usv2"
        numeric_params = [
            bigquery.ScalarQueryParameter("usv1", "INT64", num_min),
            bigquery.ScalarQueryParameter("usv2", "INT64", num_max)
        ]
    else:
        num_value = int(input("عدد سریال: "))
        numeric_sql = f"UserServiceId {op} @usv1"
        numeric_params = [bigquery.ScalarQueryParameter("usv1", "INT64", num_value)]

filter_date = input("آیا می‌خواهی تاریخ فیلتر شود؟ (y/n): ").strip().lower() == 'y'
date_sql, date_params = None, []
if filter_date:
    date_op = input("نوع فیلتر (مثلاً خاص=EXACT، بازه=BETWEEN): ")
    if date_op == "EXACT":
        date_value = input("تاریخ (YYYY-MM-DD): ")
        date_sql = "CreatDate = @dt1"
        date_params = [bigquery.ScalarQueryParameter("dt1", "DATE", date_value)]
    else:
        date_start = input("تاریخ شروع (YYYY-MM-DD): ")
        date_end = input("تاریخ پایان (YYYY-MM-DD): ")
        date_sql = "CreatDate BETWEEN @dt1 AND @dt2"
        date_params = [
            bigquery.ScalarQueryParameter("dt1", "DATE", date_start),
            bigquery.ScalarQueryParameter("dt2", "DATE", date_end)
        ]

# ======== شروع جستجو ========

total_df = []
info_tables = []

for creator in selected_creators:
    conditions = ["Creator = @creator"]
    params = [bigquery.ScalarQueryParameter("creator", "STRING", creator)]
    if numeric_sql:
        conditions.append(numeric_sql)
        params += numeric_params
    if date_sql:
        conditions.append(date_sql)
        params += date_params
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query_base = "SELECT * FROM {table_path} " + where_clause
    df, used_table = find_creator_data(creator, query_base, params, tables_priority)
    if not df.empty:
        total_df.append(df)
        info_tables.append(f"{creator} ← {used_table}")

if total_df:
    final_df = pd.concat(total_df, ignore_index=True)
    print("\nنتیجه جستجو:")
    print(final_df)
    print("\nهر Creator از کدام جدول آمد:")
    print("\n".join(info_tables))
    # مجموع فروش و تعداد
    total_package = final_df['Package'].astype(float).sum() if 'Package' in final_df.columns else 0
    count_usv = final_df['UserServiceId'].count() if 'UserServiceId' in final_df.columns else 0
    print(f"\nمجموع فروش: {total_package:,.2f}")
    print(f"تعداد بسته‌ها: {count_usv}")
    # خروجی PDF هم اختیاری
    save_pdf = input("خروجی PDF هم ذخیره شود؟ (y/n): ").strip().lower() == 'y'
    if save_pdf:
        export_df_to_pdf(final_df, "output.pdf")
else:
    print("داده‌ای یافت نشد.")
