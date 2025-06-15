import os
import glob
import pandas as pd
from datetime import datetime

# ---------- 参数设置 ----------
DATA_DIR = "results"
HISTORY_PREFIX = "all_domains_url_details_part"
HISTORY_LIMIT_MB = 90
INDEX_FILE = os.path.join(DATA_DIR, "index.csv")
SUMMARY_FILE = os.path.join(DATA_DIR, "summary.csv")
TODAY = datetime.today().strftime("%Y-%m-%d")
NEW_URL_FILE = os.path.join(DATA_DIR, f"newurl_{TODAY}.csv")


# ---------- 加载新数据 ----------
def load_today_data():
    # 可以根据实际来源读取今日数据，例如直接从网络或变量传入，这里以文件模拟
    today_raw_file = f"raw_data_{TODAY}.csv"  # 替换成实际变量或文件名
    if not os.path.exists(today_raw_file):
        print(f"❌ 未找到今日数据文件：{today_raw_file}")
        return pd.DataFrame()
    df = pd.read_csv(today_raw_file).drop_duplicates(subset=["loc"])
    df["added_date"] = TODAY
    return df


# ---------- 加载历史数据并合并 ----------
def load_full_history():
    part_files = sorted(glob.glob(os.path.join(DATA_DIR, f"{HISTORY_PREFIX}*.csv")))
    all_parts = [pd.read_csv(f) for f in part_files]
    return pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame(columns=["loc", "lastmodified", "added_date"])


# ---------- 保存历史分片 ----------
def save_partitioned_history(df):
    df = df.drop_duplicates(subset=["loc"]).reset_index(drop=True)
    current_part = 1
    start = 0
    total = len(df)
    index_entries = []

    os.makedirs(DATA_DIR, exist_ok=True)

    while start < total:
        end = min(start + 100_000, total)
        part_df = df.iloc[start:end]
        part_filename = f"{HISTORY_PREFIX}{current_part}.csv"
        part_path = os.path.join(DATA_DIR, part_filename)
        part_df.to_csv(part_path, index=False)

        index_entries.append({
            "part_file": part_filename,
            "start_index": start,
            "end_index": end - 1,
            "num_records": len(part_df),
            "last_updated": TODAY
        })
        current_part += 1
        start = end

    pd.DataFrame(index_entries).to_csv(INDEX_FILE, index=False)


# ---------- 保存今日新增 ----------
def save_new_urls(new_urls):
    new_urls.to_csv(NEW_URL_FILE, index=False)


# ---------- 更新每日统计 ----------
def update_summary(today_total, new_count, cumulative_count):
    summary_data = {
        "date": TODAY,
        "total_checked": today_total,
        "new_added": new_count,
        "cumulative_total": cumulative_count
    }

    if os.path.exists(SUMMARY_FILE):
        summary_df = pd.read_csv(SUMMARY_FILE)
        summary_df = summary_df[summary_df["date"] != TODAY]
        summary_df = pd.concat([summary_df, pd.DataFrame([summary_data])])
    else:
        summary_df = pd.DataFrame([summary_data])

    summary_df.to_csv(SUMMARY_FILE, index=False)


# ---------- 主执行逻辑 ----------
def main():
    today_data = load_today_data()
    if today_data.empty:
        return

    history_df = load_full_history()

    # 过滤出新增 URL
    if not history_df.empty:
        new_urls = today_data[~today_data["loc"].isin(history_df["loc"])]
        updated_history = pd.concat([history_df, new_urls], ignore_index=True)
    else:
        new_urls = today_data.copy()
        updated_history = new_urls.copy()

    # 保存结果
    save_new_urls(new_urls)
    save_partitioned_history(updated_history)
    update_summary(today_total=len(today_data), new_count=len(new_urls), cumulative_count=len(updated_history))

    print(f"✅ 处理完成：新增 {len(new_urls)} 条，累计 {len(updated_history)} 条")


if __name__ == "__main__":
    main()
