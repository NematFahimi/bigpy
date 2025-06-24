import streamlit as st
import pandas as pd
import jdatetime
import datetime

st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("📊 کار رو به کاردان بسپار")

uploaded_file = st.file_uploader("📁 فایل CSV خود را آپلود کنید", type=["csv"])

if uploaded_file is not None:
    df_raw = pd.read_csv(uploaded_file)
    st.write("🗂️ پیش‌نمایش داده‌های خام (۱۰ سطر اول):")
    st.dataframe(df_raw.head(10))

    if st.button("🧹 Clean Data"):
        columns_to_drop = [
            'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff',
            'CancelDT', 'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod',
            'InstallmentFirstCash', 'ServiceIsDel'
        ]
        df_clean = df_raw.drop(columns=columns_to_drop, errors='ignore')

        # انتقال ستون تاریخ "CDT" به ابتدای جدول (اگر وجود داشت)
        cols = list(df_clean.columns)
        if "CDT" in cols:
            cols.insert(0, cols.pop(cols.index("CDT")))
            df_clean = df_clean[cols]

        # تغییر نام ستون‌ها
        new_columns = [
            "CreatDate",
            "UserServiceId",
            "Creator",
            "ServiceName",
            "Username",
            "ServiceStatus",
            "ServicePrice",
            "Package",
            "StartDate",
            "EndDate"
        ]
        df_clean.columns = new_columns[:len(df_clean.columns)]

        # تابع تبدیل تاریخ به میلادی با پشتیبانی همه حالت‌ها
        def to_gregorian_if_jalali(date_str):
            try:
                if not isinstance(date_str, str):
                    return date_str
                # اگر تاریخ شمسی است (۱۴xx)
                if date_str.startswith('14'):
                    parts = date_str.replace('-', '/').split('/')
                    if len(parts) == 3:
                        jy, jm, jd = map(int, parts)
                        gdate = jdatetime.date(jy, jm, jd).togregorian()
                        return gdate.strftime('%Y-%m-%d')
                # اگر تاریخ میلادی است (۲۰xx)
                elif date_str.startswith('20'):
                    parts = date_str.replace('-', '/').split('/')
                    if len(parts) == 3:
                        gy, gm, gd = map(int, parts)
                        return datetime.date(gy, gm, gd).strftime('%Y-%m-%d')
                return date_str
            except Exception:
                return date_str

        # اعمال تابع روی ستون CreatDate
        df_clean['CreatDate'] = df_clean['CreatDate'].apply(to_gregorian_if_jalali)

        st.success("✅ پاکسازی و تبدیل تاریخ انجام شد! ۱۰ سطر اول:")
        st.dataframe(df_clean.head(10))
