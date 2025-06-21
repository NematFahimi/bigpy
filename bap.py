import streamlit as st
import pandas as pd
import numpy as np
from io import StringIO

st.set_page_config(page_title="پردازش فایل CSV خدمات کاربران", layout="wide")

st.title("🧾 برنامه پردازش گزارش خدمات کاربران")

uploaded_file = st.file_uploader("📤 فایل CSV را آپلود کنید", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    
    st.success("✅ فایل با موفقیت خوانده شد.")
    st.write("پیش‌نمایش فایل اصلی:", df.head())

    # حذف ستون‌ها
    columns_to_drop = [
        'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff', 'CancelDT',
        'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod', 'InstallmentFirstCash', 'ServiceIsDel'
    ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    # دریافت UserServiceId از کاربر
    user_input = st.number_input("🔢 لطفاً شماره UserServiceId را وارد کنید:", min_value=1, step=1)

    if st.button("🚀 پردازش فایل"):
        # حذف ردیف‌ها تا و شامل UserServiceId
        index_target = df.index[df['UserServiceId'] == user_input].tolist()
        if not index_target:
            st.error(f"UserServiceId برابر {user_input} پیدا نشد.")
        else:
            start_index = index_target[0] + 1
            df = df.loc[start_index:].reset_index(drop=True)
            st.info(f"تمام ردیف‌های قبل و شامل UserServiceId={user_input} حذف شدند.")

            # پاک‌سازی مقادیر
            if 'ServicePrice' in df.columns:
                df['ServicePrice'] = np.nan
            if 'SavingOffUsed' in df.columns:
                df['SavingOffUsed'] = np.nan

            # فرمت‌دهی تاریخ CDT
            def format_gregorian_date_str(date_str):
                try:
                    date_part = str(date_str).split(' ')[0]
                    date = pd.to_datetime(date_part, errors='coerce')
                    return date.strftime('%Y-%m-%d') if pd.notnull(date) else None
                except:
                    return None

            if 'CDT' in df.columns:
                df['CDT'] = df['CDT'].apply(format_gregorian_date_str)
                cols = list(df.columns)
                cols.insert(0, cols.pop(cols.index('CDT')))
                df = df[cols]

            # نمایش و ذخیره نهایی
            st.success("✅ فایل با موفقیت پردازش شد.")
            st.write("پیش‌نمایش خروجی:", df.head())

            # دانلود فایل نهایی
            csv = df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 دانلود فایل نهایی CSV",
                data=csv,
                file_name='final_output.csv',
                mime='text/csv'
            )
