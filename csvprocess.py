import pandas as pd
import jdatetime
import numpy as np

# --- مرحله اول: خواندن فایل CSV ---
file_path = r'C:\Users\FAHIMI\Downloads\ServiceReport.CSV'  # مسیر فایل ورودی را اینجا وارد کن
df = pd.read_csv(file_path)

# --- مرحله دوم: حذف کالم‌های مشخص ---
columns_to_drop = [
    'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff', 'CancelDT',
    'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod', 'InstallmentFirstCash', 'ServiceIsDel'
]
df = df.drop(columns=columns_to_drop)

# --- مرحله سوم: حذف سطرهای قبل از UserServiceId خاص ---
target_id = int(input("لطفا یک شماره UserServiceId وارد کنید: "))
index_target = df.index[df['UserServiceId'] == target_id].tolist()

if not index_target:
    print(f"UserServiceId برابر {target_id} پیدا نشد.")
else:
    start_index = index_target[0]
    df = df.loc[start_index:].reset_index(drop=True)
    print(f"تمام سطرهای قبل از UserServiceId={target_id} حذف شدند.")

# --- مرحله جدید: پاک کردن مقادیر ستون‌های ServicePrice و SavingOffUsed ---
df['ServicePrice'] = np.nan
df['SavingOffUsed'] = np.nan

# --- مرحله چهارم: تبدیل تاریخ شمسی به میلادی و انتقال ستون CDT به ابتدا ---
def persian_to_gregorian_str(persian_datetime_str):
    try:
        # جدا کردن فقط بخش تاریخ از رشته (قبل از اولین فاصله)
        persian_date_str = persian_datetime_str.split(' ')[0]

        year, month, day = map(int, persian_date_str.split('/'))
        gregorian_date = jdatetime.date(year, month, day).togregorian()
        return gregorian_date.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"خطا در تبدیل تاریخ: {persian_datetime_str} -> {e}")
        return None

df['CDT'] = df['CDT'].apply(persian_to_gregorian_str)

cols = list(df.columns)
cols.insert(0, cols.pop(cols.index('CDT')))
df = df[cols]

# --- ذخیره فایل نهایی ---
output_path = 'final_output.csv'  # مسیر و نام فایل خروجی
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"فایل نهایی با نام '{output_path}' ذخیره شد.")
print(df.head())
