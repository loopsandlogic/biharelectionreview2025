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
load_dotenv(override=True)

base_url = "https://results.eci.gov.in/ResultAcGenNov2025/ConstituencywiseS04"

def source_url(base_url: str, seq_no: int) -> str:
    return base_url + str(seq_no) + ".htm"

def get_state_details(state: str) -> dict:
    """Returns state name, state code, and number of constituencies."""

    state_df = pd.read_csv("data/ind-state-constituency-details.csv")
    state_details = state_df[state_df["state_name"].str.lower() == state.lower()].iloc[0]
    return {
        "state_name": state_details["state_name"],
        "state_code": state_details["state_code"],
        "assembly_seats": state_details["assembly_seats"].item(),
    }

def get_coalition_parties() -> pd.DataFrame:
    return pd.read_csv("data/bihar_coalitions.csv")

def get_base_url(state: str, year: int) -> str:
    base_url_df = pd.read_csv("data/ind-state-ec-url.csv")
    base_url = base_url_df[(base_url_df["state"] == state) & (base_url_df["year"] == year)].iloc[0]["base_url"]
    return base_url

def get_voting_tally(base_url: str, total_constituency: int) -> pd.DataFrame:
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
        print(f"Scraping data for constituency number {seq_no}...")
        driver.get(source_url(base_url, seq_no))
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, "tbody")))

        h1 = driver.find_element(By.TAG_NAME, "h1").text
        h2 = driver.find_element(By.TAG_NAME, "h2").text
        candidates = driver.find_element(By.TAG_NAME, "tbody")

        state = h2.split("(")[1].replace(")", "").strip()
        constituency = h2.split("(")[0].split("-")[1].strip()
        election_year = h1.split("-")[-1].strip()
        election_type = h2.split()[0]


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
    CATALOG_NAME = os.environ["CATALOG_NAME"]
    SCHEMA_NAME = os.environ["SCHEMA_NAME"]
    VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{os.environ['VOLUME_NAME']}"

    api_endpoint = f"{DATABRICKS_HOST}/api/2.0/fs/files{VOLUME_PATH}/{remote_file}"

    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
    }

    with open(local_file, "rb") as f:
        response = requests.put(api_endpoint, headers=headers, data=f)

    print(response.status_code, response.text)

def get_state_result(state: str, voting_year: int) -> pd.DataFrame:
    total_constituencies = get_state_details(state)["assembly_seats"]
    url = get_base_url(state, voting_year)

    df = get_voting_tally(url, total_constituencies)
    coal_df = get_coalition_parties()
    return df.join(coal_df.set_index("party"), on="party", how="left")

def load_results_to_db(state: str, voting_year: int):

    start_time = datetime.now().strftime("%Y%m%d%H%M")
    print(f"{start_time}: Starting {state} Election Data Scrapper for {voting_year} Elections...")
    
    df = get_state_result(state, voting_year)

    df.to_csv(f"data/{state.lower()}_election_results_{start_time}.csv", index=False, header=True)
    
    end_time = datetime.now().strftime("%Y%m%d%H%M")
    print(f"{end_time}: {state} Election Data Scrapper completed successfully. Data saved to '{state.lower()}_election_results_{start_time}.csv'.") 


    load_to_databricks(
        remote_file=f"{state.lower()}_election_results_{start_time}.csv",
        local_file=f"data/{state.lower()}_election_results_{start_time}.csv"
    )


if __name__ == "__main__":
    load_results_to_db("Bihar", 2025)