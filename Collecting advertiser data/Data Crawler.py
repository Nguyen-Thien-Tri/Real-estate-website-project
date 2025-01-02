import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

# Define the function to retry finding elements
def find_element_with_retry(driver, by, value, retries=3, wait=2):
    for attempt in range(retries):
        try:
            element = driver.find_element(by, value)
            return element
        except NoSuchElementException:
            if attempt < retries - 1:
                time.sleep(wait)  # Wait before retrying
            else:
                raise  # Reraise the exception if all retries are exhausted

def scrape_seller_data():
    # Set up Selenium WebDriver
    driver = webdriver.Chrome()

    base_url = "https://batdongsan.com.vn/nha-moi-gioi/p"
    page_number = 1
    all_sellers = []
    batch_count = 0

    while True:
        print(f"Scraping page {page_number}...")
        driver.get(base_url + str(page_number))
        time.sleep(3)  # Allow the page to load completely

        try:
            # Locate the main content element
            content = find_element_with_retry(driver, By.XPATH, '//*[@id="contentPage"]')

            # Find all seller names
            seller_names = content.find_elements(By.CLASS_NAME, "re__broker-title--xs")

            # Find all address and phone elements
            address_phone_elements = content.find_elements(By.CLASS_NAME, "re__broker-address")

            # Process the address and phone data
            seller_addresses = address_phone_elements[0::2]  # Every other element (start with address)
            seller_phones = address_phone_elements[1::2]      # Every other element (start with phone)

            if not seller_names or not seller_addresses or not seller_phones:
                print("No more data found. Stopping scraper.")
                break

            # Combine the data
            for name, address, phone in zip(seller_names, seller_addresses, seller_phones):
                seller_data = {
                    "Name": name.text.strip(),
                    "Address": address.text.strip(),
                    "Phone": phone.text.strip()
                }
                all_sellers.append(seller_data)

            # Save data every 100 pages
            if page_number % 100 == 0:
                batch_count += 1
                df = pd.DataFrame(all_sellers)
                df.drop_duplicates(inplace=True)
                file_name = f"sellers_data_batch_{batch_count}.xlsx"
                df.to_excel(file_name, index=False)
                print(f"Batch {batch_count} saved to {file_name}")
                all_sellers = []  # Clear the list for the next batch

        except NoSuchElementException:
            print("No more data found or unable to locate elements. Stopping scraper.")
            break

        page_number += 1

    # Close the driver
    driver.quit()

    # Save remaining data if any
    if all_sellers:
        batch_count += 1
        df = pd.DataFrame(all_sellers)
        df.drop_duplicates(inplace=True)
        file_name = f"sellers_data_batch_{batch_count}.xlsx"
        df.to_excel(file_name, index=False)
        print(f"Final batch saved to {file_name}")

# Run the scraper
if __name__ == "__main__":
    scrape_seller_data()
