import os
import shutil
import time
import numpy as np
import pandas as pd
import csv
from google.cloud import bigquery
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from datetime import date, timedelta, datetime


def get_page_with_retry(url, wait=30):
    while True:
        try:
            driver.get(url)
            return
        except:
            time.sleep(wait)


def init_driver():
    # Initialize the Chrome webdriver
    options = Options()
    user_data_dir = r"C:\Users\nttzz\AppData\Local\Google\Chrome\User Data"
    profile_name = "Profile 2"
    options.add_argument(f"--user-data-dir={user_data_dir}")  # Load user data directory
    options.add_argument(f"--profile-directory={profile_name}")
    driver = webdriver.Chrome(options=options)

    # Minimize the webdriver
    driver.minimize_window()
    return driver


# Function to find an element with retries
def find_element_with_retry(by, value, retries=3, wait=2):
    for _ in range(retries):
        try:
            return driver.find_element(by, value)
        except:
            time.sleep(wait)
    raise Exception(f"Element not found: {value}")


# Function to find the max page from pagination
def get_max_page():
    try:
        pagination_group = find_element_with_retry(By.CLASS_NAME, "re__pagination-group")
        near_last_child = pagination_group.find_elements(By.XPATH, "./*")[-2]
        # remove the period in the number
        return int(near_last_child.text.replace(".", ""))
    except Exception:
        return 1


# Function to find the starting page
def find_start_page(base_url, end_date, start_page, max_pages):
    mid = (start_page + max_pages) // 2
    if mid == start_page:
        return start_page
    get_page_with_retry(base_url.format(i=mid))
    product_list = find_element_with_retry(By.ID, "product-lists-web")
    children = product_list.find_elements(By.CLASS_NAME, "js__card-full-web")
    for child in children:
        element = child.find_element(By.CSS_SELECTOR, "span.re__card-published-info-published-at")
        ngay_dang = element.get_attribute("aria-label")
        ngay_dang_date = datetime.strptime(ngay_dang, "%d/%m/%Y").date()
        if ngay_dang_date <= end_date:
            return find_start_page(base_url, end_date, start_page, mid)
        elif ngay_dang_date > end_date:
            return find_start_page(base_url, end_date, mid, max_pages)


# Function to scrape links
def scrape_links(base_url, start_page, start_date, end_date, max_pages, existed_ads_ids):
    all_links = []

    for i in range(start_page - 2, max_pages + 1):
        url = base_url.format(i=i)
        get_page_with_retry(url)
        product_list = find_element_with_retry(By.ID, "product-lists-web")
        children = product_list.find_elements(By.XPATH, "./*")
        for child in children:
            element = child.find_element(By.CSS_SELECTOR, "span.re__card-published-info-published-at")
            ngay_gia_han = element.get_attribute("aria-label")
            ngay_gia_han = datetime.strptime(ngay_gia_han, "%d/%m/%Y").date()
            if ngay_gia_han > end_date:
                continue
            if ngay_gia_han < start_date:
                return all_links
            link_element = child.find_element(By.TAG_NAME, "a")
            link = link_element.get_attribute("href")
            ads_id = link.split("-")[-1].replace("pr", "")

            # TODO: Check if the ads_id is in existed_ads_ids
            if existed_ads_ids:
                if ads_id not in existed_ads_ids:
                    all_links.append((link, ngay_gia_han))
            else:
                all_links.append((link, ngay_gia_han))

    # Drop duplicate
    all_links = list(set(all_links))

    return all_links


# Function to collect ads data
def collect_ads_data(links):
    # Initialize or resume from the last saved batch
    ads_data = []
    last_batch_index = 0

    # Check the directory for existing batch files
    if not os.path.exists("ads_data"):
        os.makedirs("ads_data")
    else:
        batch_files = [f for f in os.listdir("ads_data") if f.startswith("ads_data_") and f.endswith(".xlsx")]
        if batch_files:
            # Find the last batch file index
            batch_indices = [int(f.split('_')[2].split('.')[0]) for f in batch_files]
            last_batch_index = max(batch_indices)

    # Start from the next batch
    start_index = last_batch_index * 100
    links = links[start_index:]

    for index, link in enumerate(links, start=start_index):
        get_page_with_retry(link[0])
        ngay_gia_han = link[1]

        # If the page is redirected to the homepage, skip the link
        if driver.current_url == "https://batdongsan.com.vn/":
            continue

        ad_data = {}
        ad_data["Ngày gia hạn"] = ngay_gia_han

        # Get ads features
        try:
            config_items_element = find_element_with_retry(By.CLASS_NAME, "js__pr-config")
            config_items = driver.find_elements(By.CLASS_NAME, "js__pr-config-item")
            for item in config_items:
                title_element = item.find_element(By.CLASS_NAME, "title")
                value_element = item.find_element(By.CLASS_NAME, "value")
                ad_data[title_element.text] = value_element.text
        except:
            continue

        # Get the ads type, product type, product address
        element = find_element_with_retry(By.XPATH, "/html/body/div[8]/div[1]/div[2]/div[1]/div[2]")
        childs = element.find_elements(By.TAG_NAME, "a")
        ad_data['Loại quảng cáo'] = childs[0].text
        ad_data['Loại BĐS'] = childs[0].get_attribute("title")
        ad_data['Tỉnh, thành phố'] = childs[1].text
        ad_data['Quận'] = childs[2].text
        ad_data['Khu vực'] = childs[3].text

        # Get product features
        features_element = find_element_with_retry(By.CLASS_NAME, "re__pr-specs-content")
        feature_titles = driver.find_elements(By.CLASS_NAME, "re__pr-specs-content-item-title")
        feature_values = driver.find_elements(By.CLASS_NAME, "re__pr-specs-content-item-value")
        for title, value in zip(feature_titles, feature_values):
            ad_data[title.text] = value.text

        # Get product coordinates
        map_element = find_element_with_retry(By.CLASS_NAME, "place-name")
        ad_data['Tọa độ'] = map_element.get_attribute("data-coordinates")

        # Get associated project links and names
        try:
            project_link_element = driver.find_element(By.XPATH,
                                                       '//*[@id="product-detail-web"]/div[4]/div/div[2]/div[1]/a')
            ad_data['Link dự án'] = project_link_element.get_attribute("href")
            project_name_element = driver.find_element(By.XPATH,
                                                       '//*[@id="product-detail-web"]/div[4]/div/div[2]/div[2]/div[1]/div')
            ad_data['Tên dự án'] = project_name_element.text
        except:
            ad_data['Link dự án'] = None
            ad_data['Tên dự án'] = None

        ads_data.append(ad_data)

        # Save data for each 100 links
        if (index + 1) % 100 == 0:
            df = pd.DataFrame(ads_data)
            df.to_excel(f"ads_data/ads_data_{(index // 100) + 1}.xlsx", index=False)
            ads_data = []

    # Save remaining data
    if ads_data:
        df = pd.DataFrame(ads_data)
        df.to_excel(f"ads_data/ads_data_{(index // 100) + 1}.xlsx", index=False)

    driver.quit()


def data_processing(df1):
    df = df1.copy()

    # Process "Diện tích"
    def convert_to_float(text, unit=" m²"):
        try:
            # Replace thousands separator and decimal comma
            text = text.replace('.', '').replace(',', '.')

            # Remove unit
            text = text.replace(unit, "")

            return float(text)
        except:
            return None

    df["Diện tích"] = df["Diện tích"].apply(convert_to_float)

    # Replace " phòng" with ""
    df["Số phòng ngủ"] = df["Số phòng ngủ"].str.replace(" phòng", "").astype(
        "Int64") if "Số phòng ngủ" in df.columns else None
    df["Số phòng tắm, vệ sinh"] = df["Số phòng tắm, vệ sinh"].str.replace(" phòng",
                                                                          "").astype(
        "Int64") if "Số phòng tắm, vệ sinh" in df.columns else None
    df["Số toilet"] = df["Số toilet"].str.replace(" phòng", "").astype("Int64") if "Số toilet" in df.columns else None

    # Move key columns to head columns
    columns_to_move = ["Mã tin", "Loại tin", "Ngày đăng", "Ngày gia hạn", "Ngày hết hạn", "Tọa độ"]
    remaining_columns = [col for col in df.columns if col not in columns_to_move]
    df = df[columns_to_move + remaining_columns]

    # Change "Ngày đăng", "Ngày gia hạn" and "Ngày hết hạn" (d/m/y) into date
    df["Ngày đăng"] = pd.to_datetime(df["Ngày đăng"], format="%d/%m/%Y")
    df["Ngày gia hạn"] = pd.to_datetime(df["Ngày gia hạn"], format="%d/%m/%Y")
    df["Ngày hết hạn"] = pd.to_datetime(df["Ngày hết hạn"], format="%d/%m/%Y")

    # Replace "Bán ", "Cho thuê " and " tại Việt Nam" by "" for each value in "Loại BĐS"
    df["Loại BĐS"] = df["Loại BĐS"].str.replace("Bán ", "")
    df["Loại BĐS"] = df["Loại BĐS"].str.replace("Cho thuê ", "")
    df["Loại BĐS"] = df["Loại BĐS"].str.replace("Cho thuê, sang nhượng ", "")
    df["Loại BĐS"] = df["Loại BĐS"].str.replace(" tại Việt Nam", "")

    # Process "Khu vực"
    def process_khu_vuc(text):
        try:
            return text.split(" tại ")[1]
        except:
            return None

    df["Khu vực"] = df["Khu vực"].apply(process_khu_vuc)

    # Process "Mức giá"
    for index, row in df.iterrows():
        parts = row["Mức giá"].split(" ")
        first_part = float(parts[0].replace('.', '').replace(',', '.')) if parts[0] != "Thỏa" else "Thỏa"
        second_part = parts[1]

        if second_part in ("tỷ", "tỷ/tháng"):
            df.at[index, "Mức giá"] = first_part * 1e9
        elif second_part == "thuận":
            df.at[index, "Mức giá"] = np.nan
        elif second_part == "triệu/m²":
            df.at[index, "Mức giá"] = row["Diện tích"] * first_part * 1e6
        elif second_part in ("triệu", "triệu/tháng"):
            df.at[index, "Mức giá"] = first_part * 1e6
        elif second_part == "nghìn/m²":
            df.at[index, "Mức giá"] = row["Diện tích"] * first_part * 1e3
        elif second_part == "nghìn/tháng":
            df.at[index, "Mức giá"] = first_part * 1e3
        elif second_part == "tỷ/m²":
            df.at[index, "Mức giá"] = row["Diện tích"] * first_part * 1e9
    df["Mức giá"] = df["Mức giá"].astype(float)

    # Process "Đường vào" and "Mặt tiền"
    df["Đường vào"] = df["Đường vào"].apply(
        lambda x: convert_to_float(x, unit=" m")) if "Đường vào" in df.columns else None
    df["Mặt tiền"] = df["Mặt tiền"].apply(
        lambda x: convert_to_float(x, unit=" m")) if "Mặt tiền" in df.columns else None

    # Process "Số tầng"
    df["Số tầng"] = df["Số tầng"].str.replace(" tầng", "").astype("Int64") if "Số tầng" in df.columns else None

    # Process "Mã tin"
    df["Mã tin"] = df["Mã tin"].astype(str)

    # Change column names
    column_mapping = {
        "Loại quảng cáo": "Loai_quang_cao",
        "Loại BĐS": "Loai_BDS",
        "Tỉnh, thành phố": "Tinh_thanh_pho",
        "Quận": "Quan",
        "Khu vực": "Khu_vuc",
        "Diện tích": "Dien_tich",
        "Mức giá": "Muc_gia",
        "Hướng nhà": "Huong_nha",
        "Số phòng ngủ": "So_phong_ngu",
        "Pháp lý": "Phap_ly",
        "Nội thất": "Noi_that",
        "Link dự án": "Link_du_an",
        "Tên dự án": "Ten_du_an",
        "Ngày đăng": "Ngay_dang",
        "Ngày hết hạn": "Ngay_het_han",
        "Loại tin": "Loai_tin",
        "Mã tin": "Ma_tin",
        "Hướng ban công": "Huong_ban_cong",
        "Số toilet": "So_toilet",
        "Đường vào": "Duong_vao",
        "Số tầng": "So_tang",
        "Mặt tiền": "Mat_tien",
        "Số phòng tắm, vệ sinh": "So_phong_tam_ve_sinh",
        "Tọa độ": "Toa_do",
        "Ngày gia hạn": "Ngay_gia_han"
    }
    df.rename(columns=column_mapping, inplace=True)

    # Convert all object columns to str columns
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)

    # Drop nan values
    df.dropna(subset=["Ma_tin", "Loai_tin", "Ngay_dang", "Ngay_het_han", "Loai_quang_cao", "Loai_BDS", "Tinh_thanh_pho",
                      "Quan", "Dien_tich", "Mưc_gia"], inplace=True)

    print(df.info())

    return df


def push_data_to_bigquery(data_dir="ads_data/", project_id="real-estate-project-445516", dataset_id="real_estate_data",
                          table_id="ads_data"):
    # Get the list of files in the data directory
    files = os.listdir(data_dir)

    # Initialize a DataFrame to store the data from all files
    df = pd.DataFrame()

    # Read the data from each file and append it to the DataFrame
    for file in files:
        data = pd.read_excel(f"{data_dir}/{file}")
        df = pd.concat([df, data], ignore_index=True)

    # Process the data
    df = data_processing(df)

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Configure job settings for appending data
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND",  # Appends data to the existing table
                                        autodetect=True  # Auto-detect schema from the DataFrame
                                        )

    # Load the DataFrame into BigQuery
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Successfully pushed data to {table_ref}")
    except Exception as e:
        print(f"Error: {e}")


def get_max_ngay_gia_han(project_id="real-estate-project-445516", dataset_id="real_estate_data", table_id="ads_data",
                      column_name="Ngay_gia_han"):
    client = bigquery.Client()

    # Construct the SQL query
    query = f"""
        SELECT MAX({column_name}) AS max_ngay_gia_han
        FROM `{project_id}.{dataset_id}.{table_id}`
    """

    try:
        # Run the query
        query_job = client.query(query)
        result = query_job.result()

        # Extract the max value
        for row in result:
            max_ngay_gia_han = row.max_ngay_gia_han  # This will be a date object if the column is a DATE type
            return max_ngay_gia_han  # Return the maximum date (or None if no rows exist)
    except Exception as e:
        print(f"Error: {e}")
        return None


def determine_start_date(project_id="real-estate-project-445516", dataset_id="real_estate_data", table_id="ads_data"):
    max_ngay_gia_han = get_max_ngay_gia_han()

    if max_ngay_gia_han:
        # Add 1 day to the max date
        start_date = max_ngay_gia_han + timedelta(days=1)
    else:
        # Raise an error if the table is empty
        raise Exception("The table is empty")

    return start_date


def clear_previous_data():
    if os.path.exists("ads_data"):
        shutil.rmtree("ads_data")
    os.makedirs("ads_data")

def get_existed_ads_ids(project_id="real-estate-project-445516", dataset_id="real_estate_data", table_id="ads_data"):
    client = bigquery.Client()

    # Construct the SQL query
    query = f"""
        SELECT DISTINCT Ma_tin
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE 
    """

    try:
        # Run the query
        query_job = client.query(query)
        result = query_job.result()

        # Extract all values
        ads_ids = [row.Ma_tin for row in result]
        return ads_ids
    except Exception as e:
        print(f"Error: {e}")
        return None

def scrape_links_wrapper():
    base_urls = ["https://batdongsan.com.vn/nha-dat-ban/p{i}?sortValue=1",
                 "https://batdongsan.com.vn/nha-dat-cho-thue/p{i}?sortValue=1"]
    # start_date = determine_start_date()
    start_date = date.today() - timedelta(days=1)
    end_date = date.today() - timedelta(days=1)
    existed_ads_ids = get_existed_ads_ids()
    all_scraped_links = []

    for base_url in base_urls:
        get_page_with_retry(base_url.format(i=1))
        max_pages = get_max_page()
        start_page = find_start_page(base_url, end_date, 1, max_pages)
        links = scrape_links(base_url, start_page, start_date, end_date, max_pages, existed_ads_ids)
        all_scraped_links.extend(links)

    # Delete scraped_links.csv
    if os.path.exists("scraped_links.csv"):
        os.remove("scraped_links.csv")

    # Save links and ngay_gia_han to a csv file
    with open("scraped_links.csv", mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(all_scraped_links)

def collect_ads_data_wrapper():
    all_scraped_links = []
    with open("scraped_links.csv", mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            all_scraped_links.append((row[0], datetime.strptime(row[1], "%Y-%m-%d").date()))
    collect_ads_data(all_scraped_links)


# Set the environment variable for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "real-estate-project-445516-83dc50b692bc.json"

driver = init_driver()

# Main script
if __name__ == "__main__":
    clear_previous_data()
    scrape_links_wrapper()
    collect_ads_data_wrapper()
    push_data_to_bigquery()
