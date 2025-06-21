import pandas as pd
import numpy as np

# --- مرحله اول: خواندن فایل CSV ---
file_path = r'C:\Users\FAHIMI\Downloads\ServiceReport.CSV'  # مسیر فایل ورودی
df = pd.read_csv(file_path)

# --- مرحله دوم: حذف کالم‌های مشخص ---
columns_to_drop = [
    'PayPlan', 'DirectOff', 'VAT', 'PayPrice', 'Off', 'SavingOff', 'CancelDT',
    'ReturnPrice', 'InstallmentNo', 'InstallmentPeriod', 'InstallmentFirstCash', 'ServiceIsDel'
]
df = df.drop(columns=columns_to_drop)

# --- مرحله سوم: حذف سطرهای قبل و شامل UserServiceId مشخص ---
target_id = int(input("لطفا یک شماره UserServiceId وارد کنید: "))
index_target = df.index[df['UserServiceId'] == target_id].tolist()

if not index_target:
    print(f"UserServiceId برابر {target_id} پیدا نشد.")
else:
    start_index = index_target[0] + 1  # حذف خود UserServiceId هم انجام شود
    df = df.loc[start_index:].reset_index(drop=True)
    print(f"تمام سطرهای قبل و شامل UserServiceId={target_id} حذف شدند.")

# --- مرحله چهارم: پاک کردن مقادیر ServicePrice و SavingOffUsed ---
df['ServicePrice'] = np.nan
df['SavingOffUsed'] = np.nan

# --- مرحله پنجم: فرمت‌دهی تاریخ میلادی به YYYY-MM-DD و انتقال ستون CDT به اول ---
def format_gregorian_date_str(date_str):
    try:
        date_part = date_str.split(' ')[0]
        date = pd.to_datetime(date_part, errors='coerce')
        return date.strftime('%Y-%m-%d') if pd.notnull(date) else None
    except Exception as e:
        print(f"خطا در فرمت‌دهی تاریخ: {date_str} -> {e}")
        return None

df['CDT'] = df['CDT'].apply(format_gregorian_date_str)

# انتقال ستون CDT به ابتدای جدول
cols = list(df.columns)
cols.insert(0, cols.pop(cols.index('CDT')))
df = df[cols]

# --- ذخیره فایل نهایی ---
output_path = 'final_output.csv'  # مسیر و نام فایل خروجی
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"\n✅ فایل نهایی با نام '{output_path}' ذخیره شد.\n")
print(df.head())
