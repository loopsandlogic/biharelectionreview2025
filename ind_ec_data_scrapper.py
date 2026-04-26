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
    state_details = state_df[state_df["State"].str.lower() == state.lower()].iloc[0]
    return {
        "State": state_details["State"],
        "State_Code": state_details["State_Code"],
        "Assembly_Seats": state_details["Assembly_Seats"].item(),
    }

def get_coalition_parties(year: int, state: str) -> pd.DataFrame:
    state = state.title()
    df = pd.read_csv("data/ind-state-coalitions.csv")
    df = df[(df["Year"] == year) & (df["State"] == state)].drop(["State", "Year"], axis=1)

    return df

def get_base_url(state: str, year: int) -> str:
    state = state.title()
    base_url_df = pd.read_csv("data/ind-state-ec-url.csv")
    base_url = base_url_df[(base_url_df["state"] == state) & (base_url_df["year"] == year)].iloc[0]["base_url"]
    return base_url

def get_state_constituency_details(state: str) -> pd.DataFrame:
    state = state.title()
    df = pd.read_csv("data/constituency_details.csv")
    df = df[["Constituency_Num", "District", "Constituency_Category"]]

    return df

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
        print(f"Scraping data for constituency number {seq_no} ...")
        driver.get(source_url(base_url, seq_no))
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, "tbody")))

        h1 = driver.find_element(By.TAG_NAME, "h1").text
        h2 = driver.find_element(By.TAG_NAME, "h2").text
        candidates = driver.find_element(By.TAG_NAME, "tbody")

        state = h2.split("(")[1].replace(")", "").strip()
        Constituency_Name = h2.split("(")[0].split("-")[1].strip()

        elem = driver.find_element(By.CSS_SELECTOR, "div.round-status")
        total_round = elem.text.split("/")[-1]
        curr_round = elem.text.split("/")[0].split()[-1]

        fieldnames = ["serial_num", "candidate_name", "party", "evmvotes", "postalvotes", "totalvotes", "vote_percent"]

        for candidate in candidates.find_elements(By.TAG_NAME, "tr"):
            results["Constituency_Num"] = seq_no
            results["Constituency_Name"] = Constituency_Name
            
            voting_tally = dict(zip(fieldnames, map(lambda d: d.text, candidate.find_elements(By.TAG_NAME, "td"))))
            results = results | voting_tally

            results["Total_Rounds"] = total_round
            results["Current_Round"] = curr_round
            results["State"] = state

            results_df = pd.DataFrame([results])
            df = pd.concat([df, results_df], ignore_index=True)

        seq_no += 1
    
    df["Constituency_Name"] = df["Constituency_Name"].str.title()
    df["candidate_name"] = df["candidate_name"].str.title()
    df = df.rename(columns={"candidate_name": "Candidate_Name",
                            "party": "Party_Name",
                            "evmvotes": "EVM_Votes",
                            "postalvotes": "Postal_Votes",
                            "totalvotes": "Total_Votes",
                            "vote_percent": "Vote_Percent",
                            "serial_num": "Serial_Num"})

    driver.quit()

    cd = get_state_constituency_details(state)

    df = df.merge(cd, left_on="Constituency_Num", right_on="Constituency_Num", how="left")

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
    total_constituencies = get_state_details(state)["Assembly_Seats"]
    url = get_base_url(state, voting_year)

    df = get_voting_tally(url, total_constituencies)
    coal_df = get_coalition_parties(voting_year, state)
    df = df.merge(coal_df, left_on="Party_Name", right_on="Party_Name", how="left")
    df["Election_Year"] = voting_year
    return df

def load_results_to_db(state: str, voting_year: int):

    start_time = datetime.now().strftime("%Y%m%d%H%M")
    print(f"{start_time}: Starting {state} Election Data Scrapper for {voting_year} Elections...")
    
    df = get_state_result(state, voting_year)

    df.to_csv(f"data/{state.lower()}_election_results_{voting_year}.csv", index=False, header=True)
    
    end_time = datetime.now().strftime("%Y%m%d%H%M")
    print(f"{end_time}: {state} Election Data Scrapper completed successfully. Data saved to '{state.lower()}_election_results_{start_time}.csv'.") 


    load_to_databricks(
        remote_file=f"{state.lower()}_election_results_{voting_year}.csv",
        local_file=f"data/{state.lower()}_election_results_{voting_year}.csv"
    )


if __name__ == "__main__":
    load_results_to_db("Bihar", 2025)