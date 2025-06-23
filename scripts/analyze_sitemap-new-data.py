import os
import glob
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from urllib.parse import urlparse
from collections import Counter
import re

# --- 配置区 ---
# 使用Seaborn美化图表
sns.set_theme(style="whitegrid")

# 内容策略分类规则 (可根据您的实际情况扩展)
# 键是分类名，值是用于匹配URL路径的正则表达式
CONTENT_CATEGORIES = {
    'Blog/Article': r'/blog/|/post/|/article/|/news/',
    'Product/Service': r'/product/|/item/|/service/|/detail/',
    'Documentation/Help': r'/docs/|/help/|/support/|/guide/',
    'User/Profile': r'/user/|/profile/|/author/',
    'Tag/Category': r'/tag/|/category/',
    'Game/App': r'/game/|/app/' # 针对itch.io的特殊分类
}

# 从URL slug中提取关键词时要忽略的常见词
URL_STOPWORDS = {'a', 'an', 'the', 'and', 'or', 'in', 'on', 'of', 'for', 'to', 'with', 'www', 'http', 'https', 'html', 'htm', 'php', 'asp'}


def analyze_overall_growth(results_folder, output_folder):
    """分析所有域名的总体每日新增URL数量。"""
    print("\n--- 1. 正在分析总体增长趋势 ---")
    stats_file = os.path.join(results_folder, 'daily_url_stats.csv')
    if not os.path.exists(stats_file):
        print(f"错误: 统计文件未找到 {stats_file}")
        return

    df = pd.read_csv(stats_file)
    if df.empty:
        print("统计文件为空，跳过分析。")
        return

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # 生成统计摘要
    total_days = len(df)
    total_urls = df['new_url_count'].sum()
    avg_urls_per_day = df['new_url_count'].mean()
    max_day = df.loc[df['new_url_count'].idxmax()]

    print(f"数据覆盖天数: {total_days}")
    print(f"累计新增URL总数: {total_urls:,}")
    print(f"平均每日新增URL: {avg_urls_per_day:.2f}")
    print(f"新增最多的一天: {max_day['date'].strftime('%Y-%m-%d')} (新增 {int(max_day['new_url_count']):,} 个)")

    # 绘制图表
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['new_url_count'], marker='o', linestyle='-')
    plt.title('Daily New URLs - Overall Growth Trend')
    plt.xlabel('Date')
    plt.ylabel('Number of New URLs')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    output_path = os.path.join(output_folder, '1_overall_growth_trend.png')
    plt.savefig(output_path)
    plt.close()
    print(f"图表已保存至: {output_path}")


def load_all_new_urls(results_folder):
    """加载所有newurl_*.csv文件并合并。"""
    new_url_files = glob.glob(os.path.join(results_folder, 'newurl_*.csv'))
    if not new_url_files:
        return None

    all_dfs = [pd.read_csv(f) for f in new_url_files]
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # 提取域名
    combined_df['domain'] = combined_df['loc'].apply(lambda url: urlparse(url).netloc)
    return combined_df


def analyze_competitive_growth(df, output_folder):
    """按域名分析和对比内容增长速度。"""
    print("\n--- 2. 正在分析各域名增长对比 ---")
    if df is None or df.empty:
        print("无新增URL数据，跳过分析。")
        return

    # 按域名统计总新增量
    domain_counts = df['domain'].value_counts()
    print("各域名累计新增URL数量 Top 10:")
    print(domain_counts.head(10).to_string())

    # 绘制Top N域名的图表
    top_n = 10
    top_domains = domain_counts.head(top_n).index
    
    df_top = df[df['domain'].isin(top_domains)]
    
    # 按天和域名统计
    daily_domain_counts = df_top.groupby(['added_date', 'domain']).size().unstack(fill_value=0)
    
    plt.figure(figsize=(15, 8))
    daily_domain_counts.plot(kind='line', marker='.', ax=plt.gca())
    plt.title(f'Daily New URLs by Top {top_n} Domains')
    plt.xlabel('Date')
    plt.ylabel('Number of New URLs')
    plt.xticks(rotation=45)
    plt.legend(title='Domain', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    output_path = os.path.join(output_folder, '2_competitive_growth_comparison.png')
    plt.savefig(output_path)
    plt.close()
    print(f"图表已保存至: {output_path}")


def analyze_content_strategy(df, output_folder):
    """分析URL路径以确定内容类型策略。"""
    print("\n--- 3. 正在分析内容策略 ---")
    if df is None or df.empty:
        print("无新增URL数据，跳过分析。")
        return

    def categorize_url(url):
        path = urlparse(url).path
        for category, pattern in CONTENT_CATEGORIES.items():
            if re.search(pattern, path, re.IGNORECASE):
                return category
        return 'Other'

    df['category'] = df['loc'].apply(categorize_url)
    category_dist = df['category'].value_counts()

    print("新增URL内容类型分布:")
    print(category_dist.to_string())

    # 绘制图表
    plt.figure(figsize=(10, 7))
    sns.barplot(x=category_dist.values, y=category_dist.index, palette="viridis")
    plt.title('Distribution of New URLs by Content Category')
    plt.xlabel('Number of New URLs')
    plt.ylabel('Category')
    plt.tight_layout()

    output_path = os.path.join(output_folder, '3_content_strategy_distribution.png')
    plt.savefig(output_path)
    plt.close()
    print(f"图表已保存至: {output_path}")


def analyze_trending_topics(df, output_folder):
    """从URL slug中提取并分析热门话题关键词。"""
    print("\n--- 4. 正在挖掘热门话题关键词 ---")
    if df is None or df.empty:
        print("无新增URL数据，跳过分析。")
        return
        
    def extract_keywords(url):
        try:
            path = urlparse(url).path
            # 获取最后一个路径片段（slug）
            slug = path.strip('/').split('/')[-1]
            # 分割并清洗
            tokens = re.split(r'[-_.,]', slug)
            keywords = [token.lower() for token in tokens if token.isalpha() and token.lower() not in URL_STOPWORDS]
            return keywords
        except Exception:
            return []

    # 使用.explode()将列表展开为多行，便于计数
    all_keywords = df['loc'].apply(extract_keywords).explode().dropna()

    if all_keywords.empty:
        print("未能从URL中提取任何有效关键词。")
        return

    keyword_counts = Counter(all_keywords)
    
    top_n = 25
    print(f"Top {top_n} 热门关键词:")
    # 将Counter转换为DataFrame以便于打印
    top_keywords_df = pd.DataFrame(keyword_counts.most_common(top_n), columns=['Keyword', 'Frequency'])
    print(top_keywords_df.to_string(index=False))

    # 绘制图表
    plt.figure(figsize=(12, 8))
    sns.barplot(x='Frequency', y='Keyword', data=top_keywords_df, palette='rocket')
    plt.title(f'Top {top_n} Trending Keywords from URL Slugs')
    plt.xlabel('Frequency')
    plt.ylabel('Keyword')
    plt.tight_layout()
    
    output_path = os.path.join(output_folder, '4_trending_keywords.png')
    plt.savefig(output_path)
    plt.close()
    print(f"图表已保存至: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Analyze sitemap scraping results to find trends and insights.")
    parser.add_argument(
        '--results-dir', 
        type=str, 
        default='results',
        help="The directory containing the sitemap scraper's output files (e.g., 'results')."
    )
    parser.add_argument(
        '--output-dir', 
        type=str, 
        default='analysis_reports',
        help="The directory where analysis reports and charts will be saved."
    )
    args = parser.parse_args()

    results_folder = args.results_dir
    output_folder = args.output_dir
    
    if not os.path.isdir(results_folder):
        print(f"错误: 结果文件夹 '{results_folder}' 不存在。")
        return

    # 创建输出文件夹
    os.makedirs(output_folder, exist_ok=True)
    print(f"分析报告将保存在: '{output_folder}'")

    # --- 执行各项分析 ---
    
    # 1. 总体增长分析
    analyze_overall_growth(results_folder, output_folder)
    
    # 加载一次所有新增URL数据，供后续函数使用，避免重复读取
    all_new_urls_df = load_all_new_urls(results_folder)

    # 2. 竞争对比分析
    analyze_competitive_growth(all_new_urls_df, output_folder)

    # 3. 内容策略分析
    analyze_content_strategy(all_new_urls_df, output_folder)

    # 4. 热门话题分析
    analyze_trending_topics(all_new_urls_df, output_folder)

    print("\n✅ 分析完成！")

if __name__ == '__main__':
    main()
