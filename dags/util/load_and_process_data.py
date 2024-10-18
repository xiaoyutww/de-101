import os
import re
import requests
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, year

data_source_directory = '/Users/xiaoyu.liu/Learning/LearnData/data_source/'
daily_output_path = '/Users/xiaoyu.liu/Learning/LearnData/output/daily.csv'
yearly_output_path = '/Users/xiaoyu.liu/Learning/LearnData/output/yearly.csv'

def generate_quarter_list():
    quarters = []
    for year in range(2019, 2024):
        for q in range(1, 5):
            if year == 2023 and q > 3:
                break
            quarters.append(f"Q{q}_{year}")
    print(quarters)
    return quarters


def check_existing_files():
    quarter_list = generate_quarter_list()
    missing_quarter_list = quarter_list
    pattern = re.compile(r'Q[1-4]_\d{4}')


    for item in os.listdir(data_source_directory):
        match = pattern.search(item)
        if match:
            if match.group() in quarter_list:
                print(f"找到匹配文件或文件夹：{item}")
                missing_quarter_list.remove(match.group())
    print(f"没找到的文件或文件夹：{missing_quarter_list}")
    return missing_quarter_list

def download_and_unzip_files():
    download_urls = []
    base_url = 'https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_'
    missing_quarter_list = check_existing_files()
    for quarter in missing_quarter_list:
        download_url = f"{base_url}{quarter}.zip"  # 根据实际的 URL 格式调整后缀
        download_urls.append(download_url)
    # print(download_urls)
        file_name = os.path.basename(download_url)
        file_path = os.path.join(data_source_directory, file_name)
        print("正在下载： ", file_name)
        response = requests.get(download_url)

        # 检查请求是否成功
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"成功下载：{file_name}")
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(data_source_directory)
            print(f"成功解压：{file_name}")
        else:
            print(f"下载失败：{download_url} (状态码: {response.status_code})")

def process_data():
    spark = (SparkSession.builder.appName('trySpark').getOrCreate())
    # 直接读取整个目录中的所有 CSV 文件
    df_combined = spark.read.csv(data_source_directory + '*/*.csv', header=True, inferSchema=True)

    daily_result = df_combined.groupBy("date") \
        .agg(
            count("*").alias("drives count"),
            count(when(col("failure") == 1, True)).alias("failure")
        ).orderBy(col("date"))
    daily_result.write.csv(daily_output_path, header=True, mode="overwrite")
    print("daily success")

    yearly_result = (df_combined.groupBy(year(col("date")).alias("year"),
                                        when(col("model").startswith("CT"), "Crucial")
                                        .when(col("model").startswith("DELLBOSS"), "Dell BOSS")
                                        .when(col("model").startswith("HGST"), "HGST")
                                        .when(col("model").startswith("ST"), "Seagate")
                                        .when(col("model").startswith("Seagate"), "Seagate")
                                        .when(col("model").startswith("Toshiba"), "TOSHIBA")
                                        .when(col("model").startswith("WDC"), "Western Digital")
                                        .otherwise("Others").alias("brand")) \
        .agg(
        count(when(col("failure") == 1, True)).alias("failures")).orderBy("year"))
    yearly_result.write.csv(yearly_output_path, header=True, mode="overwrite")
    print("yearly success")
    spark.stop()