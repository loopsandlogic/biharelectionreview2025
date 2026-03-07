from election_data_scrapper import load_to_databricks

load_to_databricks(
        remote_file="vote_details_constituency_wise_2025.csv",
        local_file="vote_details_constituency_wise_2025.csv"
    )