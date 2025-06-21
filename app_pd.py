import streamlit as st
import pandas as pd
import jdatetime
import numpy as np
from io import StringIO

st.set_page_config(page_title="📊 پردازش فایل خدمات کاربران", layout="wide")
st.title("📊 پردازش فایل گزارش خدمات")

st.markdown("ابتدا فایل CSV را انتخاب کنید:")

# --- مرحله اول: آپلود فایل ---
uploaded_file = st.file_uploader("آپلود فایل CSV", type=["csv"])

if uploaded_file:
    # خواندن فایل CSV
    df = pd.read_csv(uploaded_file)

    # --- مرحله دوم: حذف کالم‌های اضافی ---
    columns_to_drop = [
        'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOffUsed', 'CancelDT',
        'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod', 'InstallmentFirstCash', 'ServiceIsDel'
    ]
    df = df.drop(columns=columns_to_drop, errors='ignore')

    # --- مرحله سوم: دریافت UserServiceId هدف ---
    user_service_id = st.number_input("یک شماره UserServiceId وارد کنید:", min_value=1, step=1)

    if st.button("اجرای پردازش"):
        index_target = df.index[df['UserServiceId'] == user_service_id].tolist()
        if not index_target:
            st.error(f"UserServiceId برابر {user_service_id} پیدا نشد.")
        else:
            start_index = index_target[0]
            df = df.loc[start_index:].reset_index(drop=True)
            st.success(f"تمام سطرهای قبل از UserServiceId={user_service_id} حذف شدند.")

            # --- مرحله چهارم: پاک کردن قیمت‌ها ---
            df['ServicePrice'] = np.nan
            df['SavingOffUsed'] = np.nan

            # --- مرحله پنجم: تبدیل تاریخ ---
            def persian_to_gregorian_str(persian_datetime_str):
                try:
                    date_part = str(persian_datetime_str).split(' ')[0]
                    year, month, day = map(int, date_part.split('/'))
                    gdate = jdatetime.date(year, month, day).togregorian()
                    return gdate.strftime('%Y-%m-%d')
                except:
                    return None

            df['CDT'] = df['CDT'].apply(persian_to_gregorian_str)

            # انتقال ستون CDT به اول
            cols = df.columns.tolist()
            cols.insert(0, cols.pop(cols.index('CDT')))
            df = df[cols]

            # --- نمایش نتایج و دانلود ---
            st.subheader("پیش‌نمایش داده نهایی:")
            st.dataframe(df.head(15), use_container_width=True)

            csv = df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button("📥 دانلود فایل نهایی CSV", data=csv, file_name="final_output.csv", mime='text/csv')
else:
    st.info("لطفاً فایل CSV را بارگذاری کنید تا پردازش آغاز شود.")
