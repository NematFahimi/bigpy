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
        return re.search(r"\d{4}/\d{2}/\d{2}", date_str) is not None
    except:
        return False

def jalali_to_gregorian(date_str):
    try:
        match = re.search(r"(\d{4}/\d{2}/\d{2})", str(date_str))
        if not match:
            return None
        y, m, d = map(int, match.group(1).split("/"))
        g_date = jdatetime.date(y, m, d).togregorian()
        return g_date.strftime('%Y-%m-%d')
    except:
        return None

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        if df is None or df.empty:
            st.warning("فایل آپلود شده خالی است یا داده‌ای خوانده نشد!")
            st.stop()
        st.success("✅ فایل با موفقیت خوانده شد.")
        st.write("پیش‌نمایش فایل اصلی:", df.head())
    except Exception as e:
        st.error(f"خطا در خواندن فایل: {e}")
        st.stop()

    columns_to_drop = [
        'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff', 'CancelDT',
        'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod', 'InstallmentFirstCash', 'ServiceIsDel'
    ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    if 'UserServiceId' not in df.columns:
        st.error("ستون UserServiceId در فایل موجود نیست.")
        st.stop()

    user_input = st.number_input("🔢 لطفاً شماره UserServiceId را وارد کنید:", min_value=1, step=1)

    if st.button("🚀 پردازش فایل"):
        filtered_df = df[df['UserServiceId'] >= user_input].reset_index(drop=True)
        if filtered_df.empty:
            st.error(f"هیچ سطری با UserServiceId بزرگتر یا مساوی {user_input} یافت نشد.")
            st.stop()
        else:
            df = filtered_df
            st.info(f"تمام ردیف‌هایی که UserServiceId کمتر از {user_input} داشتند حذف شدند.")

            if 'ServicePrice' in df.columns:
                df['ServicePrice'] = np.nan
            if 'SavingOffUsed' in df.columns:
                df['SavingOffUsed'] = np.nan

            # تبدیل تاریخ CDT
            if 'CDT' in df.columns:
                def convert_date(x):
                    if pd.isna(x) or x is None or str(x).strip() == "":
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
                # فقط اگر ستون CDT وجود دارد جابه‌جایی انجام شود
                if 'CDT' in df.columns:
                    cols = list(df.columns)
                    cols.insert(0, cols.pop(cols.index('CDT')))
                    df = df[cols]

            # مطمئن شو دیتافریم خروجی نه None است نه خالی
            if df is not None and not df.empty:
                st.success("✅ فایل با موفقیت پردازش شد.")
                st.write("پیش‌نمایش خروجی:", df.head())
                csv = df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 دانلود فایل نهایی CSV",
                    data=csv,
                    file_name='final_output.csv',
                    mime='text/csv'
                )
            else:
                st.warning("دیتافریم نهایی خالی است و داده‌ای برای نمایش وجود ندارد.")
