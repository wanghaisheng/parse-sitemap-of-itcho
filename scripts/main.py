import os
import csv
import glob
import datetime
import pandas as pd

def aggregate_all_domains():
    today = datetime.date.today().strftime('%Y-%m-%d')
    date_folder = f'results/{today}'
    os.makedirs(date_folder, exist_ok=True)

    # === Step 1: Load historical all_domains_url_details_part*.csv ===
    previous_data = []
    for file in glob.glob('results/*/all_domains_url_details_part*.csv'):
        try:
            df = pd.read_csv(file)
            previous_data.append(df)
        except Exception as e:
            print(f"Failed to read {file}: {e}")

    if previous_data:
        all_history_df = pd.concat(previous_data, ignore_index=True)
        all_history_df.drop_duplicates(subset=['loc'], inplace=True)
        history_set = set(all_history_df['loc'].values)
    else:
        history_set = set()

    # === Step 2: Load current day's domain_url_details_*.csv ===
    all_new_details = []
    for file in glob.glob(os.path.join(date_folder, 'domain_url_details_*.csv')):
        try:
            with open(file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    all_new_details.append(row)
        except Exception as e:
            print(f"Failed to read {file}: {e}")

    # === Step 3: Filter out duplicates ===
    new_url_details = []
    for row in all_new_details:
        if row['loc'] not in history_set:
            row['added_date'] = today
            new_url_details.append(row)

    if not new_url_details:
        print("No new URLs found today.")
        return

    # === Step 4: Save to newurl_YYYY-MM-DD.csv ===
    newurl_file = os.path.join(date_folder, f'newurl_{today}.csv')
    with open(newurl_file, 'w', encoding='utf-8', newline='') as nf:
        writer = csv.DictWriter(nf, fieldnames=['loc', 'lastmodified', 'added_date'])
        writer.writeheader()
        for d in new_url_details:
            writer.writerow(d)
    print(f"Saved {len(new_url_details)} new URLs to {newurl_file}")

    # === Step 5: Write to segmented all_domains_url_details_part*.csv ===
    output_file_base = os.path.join(date_folder, 'all_domains_url_details')
    max_size = 90 * 1024 * 1024  # 90MB
    file_index = 1

    # Find the last used index to continue appending
    while os.path.exists(f"{output_file_base}_part{file_index}.csv"):
        file_index += 1

    output_file = f"{output_file_base}_part{file_index}.csv"
    f = open(output_file, 'w', encoding='utf-8', newline='')
    writer = csv.DictWriter(f, fieldnames=['loc', 'lastmodified', 'added_date'])
    writer.writeheader()
    current_size = f.tell()

    for d in new_url_details:
        writer.writerow(d)
        if f.tell() >= max_size:
            f.close()
            file_index += 1
            output_file = f"{output_file_base}_part{file_index}.csv"
            f = open(output_file, 'w', encoding='utf-8', newline='')
            writer = csv.DictWriter(f, fieldnames=['loc', 'lastmodified', 'added_date'])
            writer.writeheader()

    f.close()
    print(f"All new URLs written to segmented part files starting with: {output_file_base}_part{file_index}.csv")
