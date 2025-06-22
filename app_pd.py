import streamlit as st
import pandas as pd
import numpy as np
import jdatetime
import re

st.set_page_config(page_title="پردازش فایل CSV خدمات کاربران", layout="wide")

st.title("🧾 برنامه پردازش گزارش خدمات کاربران")

uploaded_file = st.file_uploader("📤 فایل CSV را آپلود کنید", type=["csv"])

def is_jalali_date(date_str):
    try:
        if not isinstance(date_str, str):
            return False
        date_part = date_str.split(" ")[0]
        year = int(date_part.split("/")[0])
        return year > 1300
    except:
        return False

def jalali_to_gregorian(date_str):
    try:
        parts = date_str.split(" ")
        date_part = parts[0]
        y, m, d = map(int, date_part.split("/"))
        dt = jdatetime.date(y, m, d).togregorian()
        return dt.strftime("%Y-%m-%d")
    except:
        return None

def convert_american_datetime_to_iso(date_str):
    try:
        dt = pd.to_datetime(date_str, format="%m/%d/%Y %I:%M:%S %p", errors='coerce')
        if pd.isna(dt):
            dt = pd.to_datetime(date_str, errors='coerce')
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except:
        return None

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    
    st.success("✅ فایل با موفقیت خوانده شد.")
    st.write("پیش‌نمایش فایل اصلی:", df.head())

    columns_to_drop = [
        'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff', 'CancelDT',
        'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod', 'InstallmentFirstCash', 'ServiceIsDel'
    ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    user_input = st.number_input("🔢 لطفاً شماره UserServiceId را وارد کنید:", min_value=1, step=1)

    if st.button("🚀 پردازش فایل"):
        filtered_df = df[df['UserServiceId'] >= user_input].reset_index(drop=True)
        if filtered_df.empty:
            st.error(f"هیچ سطری با UserServiceId بزرگتر یا مساوی {user_input} یافت نشد.")
        else:
            df = filtered_df
            st.info(f"تمام ردیف‌هایی که UserServiceId کمتر از {user_input} داشتند حذف شدند.")

            if 'ServicePrice' in df.columns:
                df['ServicePrice'] = np.nan
            if 'SavingOffUsed' in df.columns:
                df['SavingOffUsed'] = np.nan

            if 'CDT' in df.columns:
                def convert_date(x):
                    if pd.isna(x):
                        return None
                    x = str(x).strip()

                    if is_jalali_date(x):
                        return jalali_to_gregorian(x)
                    else:
                        try:
                            dt = pd.to_datetime(x, errors='coerce')
                            if pd.isna(dt):
                                return None
                            return dt.strftime("%Y-%m-%d")
                        except:
                            return None

                df['CDT'] = df['CDT'].apply(convert_date)
                cols = list(df.columns)
                cols.insert(0, cols.pop(cols.index('CDT')))
                df = df[cols]

            st.success("✅ فایل با موفقیت پردازش شد.")
            st.write("پیش‌نمایش خروجی:", df.head())

            csv = df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 دانلود فایل نهایی CSV",
                data=csv,
                file_name='final_output.csv',
                mime='text/csv'
            )
