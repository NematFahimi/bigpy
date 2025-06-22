import streamlit as st
import pandas as pd
import jdatetime
import io

st.set_page_config(page_title="Service Report Processor", layout="centered")
st.title("برنامه پردازش گزارش سرویس")

# آپلود فایل
uploaded_file = st.file_uploader("فایل CSV خود را آپلود کنید", type=["csv"])

if uploaded_file is not None:
    # خواندن فایل
    df = pd.read_csv(uploaded_file)

    columns_to_drop = [
        'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff',
        'CancelDT', 'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod',
        'InstallmentFirstCash', 'ServiceIsDel'
    ]
    df = df.drop(columns=columns_to_drop, errors='ignore')

    st.write("پیش‌نمایش داده‌های خام:")
    st.dataframe(df.head())

    # گرفتن رونمبر از کاربر
    ronumber = st.number_input("لطفاً رونمبر (UserServiceId) را وارد کنید:", min_value=0, value=0)
    
    # ادامه فقط وقتی رونمبر وارد شد
    if st.button("پردازش داده"):
        df = df[df['UserServiceId'] > ronumber]
        df['SavingOffUsed'] = None
        df['ServicePrice'] = None

        # انتقال ستون 'CDT' به اول
        columns = list(df.columns)
        columns.insert(0, columns.pop(columns.index('CDT')))
        df = df[columns]

        # حذف زمان از CDT
        df['CDT'] = df['CDT'].astype(str).str.split().str[0]

        def to_gregorian_if_jalali(date_str):
            try:
                if date_str.startswith('14'):
                    parts = date_str.replace('-', '/').split('/')
                    if len(parts) == 3:
                        jy, jm, jd = map(int, parts)
                        gdate = jdatetime.date(jy, jm, jd).togregorian()
                        return gdate.strftime('%Y-%m-%d')
                return date_str
            except Exception:
                return date_str

        df['CDT'] = df['CDT'].apply(to_gregorian_if_jalali)

        st.success("پردازش انجام شد. داده نهایی:")
        st.dataframe(df.head())

        # دانلود فایل
        towrite = io.BytesIO()
        df.to_csv(towrite, index=False, encoding='utf-8-sig')
        towrite.seek(0)
        st.download_button(
            label="دانلود فایل خروجی CSV",
            data=towrite,
            file_name="ServiceReport_cleaned.csv",
            mime="text/csv"
        )
