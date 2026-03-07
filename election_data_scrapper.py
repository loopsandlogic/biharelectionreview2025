from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime
import os
import requests
from dotenv import load_dotenv
load_dotenv()


def source_url(seq_no) -> str:
    base_url = "https://results.eci.gov.in/ResultAcGenNov2025/ConstituencywiseS04"
    return base_url + str(seq_no) + ".htm"

def state_data(state_name: str) -> dict:
    df = pd.read_csv("states.csv")
    state_row = df[df["state_name"].str.lower() == state_name.lower()].iloc[0]

    return {
        "state_name": state_row["state_name"],
        "state_code": state_row["state_code"],
        "total_constituencies": state_row["assembly_seats"].item(),
    }

def get_voting_tally(total_constituency: int) -> pd.DataFrame:
    seq_no = 1

    # Initialize empty results dataframe
    df = pd.DataFrame()
    results = {}

    # Chrome browser setup with performance and headless mode enabled
    options = Options()
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.77 Safari/537.36")

    driver = webdriver.Chrome(options=options)

    while seq_no <= total_constituency:
        driver.get(source_url(seq_no))
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, "tbody")))

        h1 = driver.find_element(By.TAG_NAME, "h1").text
        h2 = driver.find_element(By.TAG_NAME, "h2").text
        candidates = driver.find_element(By.TAG_NAME, "tbody")

        state = h2.split("(")[1].replace(")", "").strip()
        constituency = h2.split("(")[0].split("-")[1].strip()
        election_year = h1.split("-")[-1].strip()
        election_type = h2.split()[0]

        total_constituencies = state_data(state)["total_constituencies"]

        fieldnames = ["serial_num", "candidate_name", "party", "evmvotes", "postalvotes", "totalvotes", "vote_percent"]

        for candidate in candidates.find_elements(By.TAG_NAME, "tr"):
            results["constituency_no"] = seq_no
            results["constituency"] = constituency
            voting_tally = dict(zip(fieldnames, map(lambda d: d.text, candidate.find_elements(By.TAG_NAME, "td"))))
            results = results | voting_tally
            results_df = pd.DataFrame([results])
            df = pd.concat([df, results_df], ignore_index=True)

        seq_no += 1
    
    df["constituency"] = df["constituency"].str.title()
    df["candidate_name"] = df["candidate_name"].str.title()

    driver.quit()
    return df

def load_to_databricks(remote_file: str, local_file: str):
    DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
    DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
    LOOKUP_PATH = os.environ["LOOKUP_PATH"]

    api_endpoint = f"{DATABRICKS_HOST}/api/2.0/fs/files{LOOKUP_PATH}{remote_file}"

    headers = {
    'Authorization': f'Bearer {DATABRICKS_TOKEN}',
    }

    with open(local_file, "rb") as f:
        response = requests.put(api_endpoint, headers=headers, data=f)

    print(response.status_code, response.text)

def main():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{current_time}: Starting Bihar Election Data Scrapper for 2025 Elections...")
    total_constituencies = state_data("Bihar")["total_constituencies"]

    df = get_voting_tally(total_constituencies) 
    df.to_csv("bihar_election_results_2025.csv", index=False, header=True)
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{current_time}: Bihar Election Data Scrapper completed successfully. Data saved to 'bihar_election_results_2025.csv'.") 

    load_to_databricks(
        remote_file="bihar_election_results_2025.csv",
        local_file="bihar_election_results_2025.csv"
    )

if __name__ == "__main__":
    main()