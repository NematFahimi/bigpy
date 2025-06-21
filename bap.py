from google.cloud import bigquery
import os
from datetime import datetime

# مسیر فایل JSON کلید را اگر لوکال هستید تنظیم کنید
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\bigquery\frsphotspots-260f77909682.json"

client = bigquery.Client()
table_path = "frsphotspots.HSP.hspdata"

def get_unique_creators():
    query = f"SELECT DISTINCT Creator FROM `{table_path}` ORDER BY Creator"
    results = client.query(query).result()
    return [row.Creator for row in results if row.Creator]

def choose_creators(creators):
    print("لیست Creatorهای موجود:\n")
    for idx, name in enumerate(creators, start=1):
        print(f"{idx}. {name}")
    print("\nمثال: برای انتخاب ali و admin بنویس: 1,2")
    while True:
        try:
            choices = input("شماره‌های Creator مورد نظر را وارد کن (با کاما): ")
            indexes = [int(i.strip()) for i in choices.split(",")]
            if all(1 <= i <= len(creators) for i in indexes):
                return [creators[i - 1] for i in indexes]
            else:
                print("شماره وارد شده معتبر نیست.")
        except ValueError:
            print("لطفاً فقط عدد وارد کن، جدا شده با ','")

def get_numeric_filter():
    print("\nشرط برای ستون UserServiceId:")
    print("1. =")
    print("2. >=")
    print("3. <=")
    print("4. BETWEEN")
    while True:
        try:
            option = int(input("شماره گزینه: "))
            if option == 1:
                val = int(input("عدد: "))
                return "UserServiceId = @usv1", [bigquery.ScalarQueryParameter("usv1", "INT64", val)]
            elif option == 2:
                val = int(input("عدد: "))
                return "UserServiceId >= @usv1", [bigquery.ScalarQueryParameter("usv1", "INT64", val)]
            elif option == 3:
                val = int(input("عدد: "))
                return "UserServiceId <= @usv1", [bigquery.ScalarQueryParameter("usv1", "INT64", val)]
            elif option == 4:
                val1 = int(input("حد پایین: "))
                val2 = int(input("حد بالا: "))
                return "UserServiceId BETWEEN @usv1 AND @usv2", [
                    bigquery.ScalarQueryParameter("usv1", "INT64", val1),
                    bigquery.ScalarQueryParameter("usv2", "INT64", val2)
                ]
            else:
                print("گزینه نامعتبر.")
        except ValueError:
            print("فقط عدد وارد کن.")

def get_date_filter():
    print("\nشرط برای ستون CreatDate:")
    print("1. = (تاریخ خاص)")
    print("2. BETWEEN (بین دو تاریخ)")
    while True:
        try:
            option = int(input("شماره گزینه: "))
            if option == 1:
                d = input("تاریخ (YYYY-MM-DD): ")
                dt = datetime.strptime(d, "%Y-%m-%d").date()
                return "CreatDate = @dt1", [bigquery.ScalarQueryParameter("dt1", "DATE", dt)]
            elif option == 2:
                d1 = input("تاریخ شروع (YYYY-MM-DD): ")
                d2 = input("تاریخ پایان (YYYY-MM-DD): ")
                dt1 = datetime.strptime(d1, "%Y-%m-%d").date()
                dt2 = datetime.strptime(d2, "%Y-%m-%d").date()
                return "CreatDate BETWEEN @dt1 AND @dt2", [
                    bigquery.ScalarQueryParameter("dt1", "DATE", dt1),
                    bigquery.ScalarQueryParameter("dt2", "DATE", dt2)
                ]
            else:
                print("گزینه نامعتبر.")
        except ValueError:
            print("فرمت تاریخ باید YYYY-MM-DD باشد.")

def query_with_summary(creators, user_service_sql, user_service_params, date_sql, date_params):
    query = f"""
    WITH filtered AS (
        SELECT
            Username,
            UserServiceId,
            Creator,
            CreatDate,
            Package,
            SUM(Package) OVER (ORDER BY CreatDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunningBalance
        FROM `{table_path}`
        WHERE Creator IN UNNEST(@creator_list)
          AND {user_service_sql}
          AND {date_sql}
    )
    SELECT
        Username,
        UserServiceId,
        Creator,
        CreatDate,
        Package,
        RunningBalance,
        NULL AS UsersCount
    FROM filtered

    UNION ALL

    SELECT
        'جمع کل' AS Username,
        NULL AS UserServiceId,
        NULL AS Creator,
        NULL AS CreatDate,
        SUM(Package) AS Package,
        NULL AS RunningBalance,
        COUNT(DISTINCT Username) AS UsersCount
    FROM filtered
    """

    all_params = [
        bigquery.ArrayQueryParameter("creator_list", "STRING", creators)
    ] + user_service_params + date_params

    job_config = bigquery.QueryJobConfig(query_parameters=all_params)
    results = client.query(query, job_config=job_config).result()

    print(f"\n📊 نتایج نهایی با RunningBalance و جمع کل:\n")
    for row in results:
        print(row)

def main():
    creators = get_unique_creators()
    if not creators:
        print("هیچ Creatorی پیدا نشد.")
        return

    selected_creators = choose_creators(creators)
    user_service_sql, user_service_params = get_numeric_filter()
    date_sql, date_params = get_date_filter()
    query_with_summary(selected_creators, user_service_sql, user_service_params, date_sql, date_params)

if __name__ == "__main__":
    main()
