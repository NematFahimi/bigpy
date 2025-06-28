import streamlit as st
from google.cloud import bigquery
import pandas as pd
from fpdf import FPDF
import copy

def safe_text(text):
    try:
        return str(text).encode('latin-1', 'ignore').decode('latin-1')
    except Exception:
        return ''

credentials_info = dict(st.secrets["gcp_service_account"])
client = bigquery.Client.from_service_account_info(credentials_info)
tables_priority = ["hspdata", "hspdata_02", "hspdata_ghor"]

def export_df_to_pdf(df, filename):
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
    col0 = df.columns[0]  # رفع اخطار آینده‌نگر
    for idx, row in df.iterrows():
        is_total_row = (
            (str(row[col0]).strip().endswith("Total")) or
            (str(row[col0]).strip().lower() == "grand total")
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

def find_creator_data_collect_all(creator, numeric_sql, numeric_value, date_sql, date_value, tables_priority):
    dfs = []
    used_tables = []
    for table_name in tables_priority:
        conditions = ["Creator = @creator"]
        params = [bigquery.ScalarQueryParameter("creator", "STRING", creator)]
        if numeric_sql:
            conditions.append(numeric_sql)
            params += copy.deepcopy(numeric_value)
        if date_sql:
            conditions.append(date_sql)
            params += copy.deepcopy(date_value)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query_base = f"SELECT * FROM frsphotspots.HSP.{table_name} {where_clause}"

        # برای دیباگ، اطلاعات کوئری را نمایش بده
        st.info(
            f"🟢 <b>جدول:</b> {table_name} | <b>Creator:</b> <code>{creator}</code> | "
            f"<b>Query:</b> <code>{query_base}</code> | <b>Params:</b> {params}",
            icon="ℹ️", unsafe_allow_html=True
        )
        try:
            res = client.query(query_base, bigquery.QueryJobConfig(query_parameters=params)).result()
            rows = [dict(row) for row in res]
            st.write(f"⬅️ تعداد رکورد: {len(rows)} از جدول {table_name} برای Creator={creator}")
            if rows:
                dfs.append(pd.DataFrame(rows))
                used_tables.append(table_name)
        except Exception as e:
            st.error(
                f"❌ خطا در جدول <b>{table_name}</b> برای Creator=<b>{creator}</b>:<br><code>{str(e)}</code>",
                unsafe_allow_html=True
            )
            continue
    if dfs:
        return pd.concat(dfs, ignore_index=True), used_tables
    return pd.DataFrame(), []

st.title("📊 پنل گزارشات فارس‌روت")

creators_input = st.text_area(
    "لطفا یوزر های که میخواهید گزارش آنرا ببینید را وارد کنید",
    placeholder="مثال: Ali, Zahra, Mohsen"
)
selected_creators = []
if creators_input.strip():
    selected_creators = [c.strip() for c in creators_input.replace('\n', ',').split(',') if c.strip()]

with st.expander("شماره مسلسل سرویس ها را وارد کنید"):
    numeric_option = st.selectbox("نوع شرط", ["بدون فیلتر", "=", ">=", "<=", "BETWEEN"])
    numeric_sql, numeric_value = None, []
    if numeric_option == "BETWEEN":
        num_min = st.number_input("حد پایین", step=1, value=0)
        num_max = st.number_input("حد بالا", step=1, value=0)
        numeric_sql = "UserServiceId BETWEEN @usv1 AND @usv2"
        numeric_value = [
            bigquery.ScalarQueryParameter("usv1", "INT64", int(num_min)),
            bigquery.ScalarQueryParameter("usv2", "INT64", int(num_max))
        ]
    elif numeric_option != "بدون فیلتر":
        num_value = st.number_input("عدد", step=1, value=0)
        numeric_sql = f"UserServiceId {numeric_option} @usv1"
        numeric_value = [bigquery.ScalarQueryParameter("usv1", "INT64", int(num_value))]

with st.expander("تاریخ را انتخاب  کنید"):
    date_option = st.selectbox("نوع فیلتر تاریخ", ["بدون فیلتر", "تاریخ خاص", "تاریخ سفارشی"])
    date_sql, date_value = None, []
    if date_option == "تاریخ خاص":
        date_value0 = st.date_input("تاریخ")
        date_sql = "CreatDate = @dt1"
        date_value = [bigquery.ScalarQueryParameter("dt1", "DATE", date_value0)]
    elif date_option == "تاریخ سفارشی":
        date_start = st.date_input("تاریخ شروع")
        date_end = st.date_input("تاریخ پایان")
        date_sql = "CreatDate BETWEEN @dt1 AND @dt2"
        date_value = [
            bigquery.ScalarQueryParameter("dt1", "DATE", date_start),
            bigquery.ScalarQueryParameter("dt2", "DATE", date_end)
        ]

st.markdown("""
<style>
div[data-testid="column"] {
    width: 33.33% !important;
    padding-left: 1.5mm !important;
    padding-right: 1.5mm !important;
}
</style>
""", unsafe_allow_html=True)
cols = st.columns([1,1,1])
with cols[0]:
    btn_show_summary = st.button("مشاهده خلاصه")
with cols[1]:
    btn_download_report = st.button("دانلود گزارش")
with cols[2]:
    btn_pivot = st.button("گزارش خلاصه")

if not selected_creators:
    st.warning("وارد کردن یوزر ضروری است")
else:
    if btn_show_summary:
        total_df = []
        info_tables = []
        for creator in selected_creators:
            df, used_tables = find_creator_data_collect_all(
                creator,
                numeric_sql, numeric_value,
                date_sql, date_value,
                tables_priority
            )
            if not df.empty:
                total_df.append(df)
                info_tables.append(f"{creator} ← {', '.join(used_tables)}")
        if total_df:
            final_df = pd.concat(total_df, ignore_index=True)
            total_package = final_df['Package'].astype(float).sum() if 'Package' in final_df.columns else 0
            count_usv = final_df['UserServiceId'].count() if 'UserServiceId' in final_df.columns else 0
            st.success(f"**مجموع فروش:** {total_package:,.2f}")
            st.success(f"**تعداد بسته‌ها:** {count_usv}")
            st.info("Creator و جدول‌ها: <br>" + "<br>".join(info_tables), unsafe_allow_html=True)
        else:
            st.warning("داده‌ای یافت نشد.")

    if btn_download_report:
        total_df = []
        info_tables = []
        for creator in selected_creators:
            df, used_tables = find_creator_data_collect_all(
                creator,
                numeric_sql, numeric_value,
                date_sql, date_value,
                tables_priority
            )
            if not df.empty:
                total_df.append(df)
                info_tables.append(f"{creator} ← {', '.join(used_tables)}")
        if total_df:
            final_df = pd.concat(total_df, ignore_index=True)
            if 'UserServiceId' in final_df.columns:
                final_df = final_df.sort_values(by='UserServiceId', ascending=True)
            st.write("جدول نتایج:", final_df)
            export_df_to_pdf(final_df, "output.pdf")
            with open("output.pdf", "rb") as pdf_file:
                st.download_button(
                    label="📥 دانلود PDF",
                    data=pdf_file,
                    file_name="output.pdf",
                    mime="application/pdf"
                )
            st.info("Creator و جدول‌ها: <br>" + "<br>".join(info_tables), unsafe_allow_html=True)
        else:
            st.warning("نتیجه‌ای یافت نشد.")
