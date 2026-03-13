# ClickBench Results: ClickHouse OSS vs Azure Data Explorer

## Environment

| | ClickHouse OSS | Azure Data Explorer |
|---|---|---|
| **SKU** | Standard E8s v5 | Standard E8s v5 |
| **Nodes** | 2 | 2 |
| **Cores/node** | 8 | 8 |
| **RAM/node** | 64 GB | 64 GB |

## Dataset

- **Benchmark**: ClickBench (https://benchmark.clickhouse.com/)
- **Table**: hits (105 columns, web-analytics page-view data)
- **Rows**: ~100M
- **Source files**: 102 gzipped CSV parts

## Load Performance

| Metric | ClickHouse OSS | ADX |
|---|---|---|
| **Load time** | 3m28s | 7m06s |
| **Data size on disk** | 13.98 GB | 15.38 GB |
| **Compression ratio** | | |

## Query Duration (seconds)

| Query | Description | ClickHouse OSS | ADX |
|---|---|---|---|
| q00 | Total row count | 0.194 | |
| q01 | Count rows with non-zero advertising engine | 0.199 | |
| q02 | Sum/avg/count of advertising engine IDs | 0.321 | |
| q03 | Date-aggregated count with advertising filter | 0.240 | |
| q04 | Unique user count with advertising filter | 0.888 | |
| q05 | Distinct user count with advertising filter | 1.063 | |
| q06 | Count distinct search phrases with advertising filter | 0.206 | |
| q07 | Distinct count with advertising engine and phrase filter | 0.208 | |
| q08 | Top 10 search phrases by count | 0.915 | |
| q09 | Top 10 search phrases with unique user count | 0.996 | |
| q10 | Top 10 search phrases by unique users | 0.347 | |
| q11 | Top search engines by count | 0.371 | |
| q12 | Top search engines by unique users for phrase | 0.999 | |
| q13 | Top search engine + phrase combos | 1.504 | |
| q14 | Top search engine + phrase + user combos | 1.042 | |
| q15 | Count by is-refresh minute intervals | 0.810 | |
| q16 | Count by minute interval with domain extraction | 2.563 | |
| q17 | Count by minute interval with full URL | 1.586 | |
| q18 | Top resolution combos | 4.688 | |
| q19 | Top referring domains for specific counter | 0.203 | |
| q20 | Count URLs containing google | 0.600 | |
| q21 | Top 10 search phrases on google URLs | 0.307 | |
| q22 | Top 10 search phrases on google URLs with min URL | 0.815 | |
| q23 | Top 10 search phrases on google URLs, URL substring | 0.776 | |
| q24 | Minute-bucketed page views for specific counter and date | 0.448 | |
| q25 | Count by page title for specific counter | 0.412 | |
| q26 | First 10 search phrases ordered by time | 0.454 | |
| q27 | First 10 search phrases ordered by time (wide select) | 0.350 | |
| q28 | Event time stats and count for google URLs | 16.036 | |
| q29 | URL count for specific counter and date range | 0.328 | |
| q30 | Count distinct users by minute for specific counter | 0.624 | |
| q31 | Count distinct users for non-refresh hits | 1.004 | |
| q32 | Minute-level distinct user counts with hour breakdown | 4.825 | |
| q33 | Referrer URL counts for random user | 4.236 | |
| q34 | Count by URL and is-download for random user | 4.184 | |
| q35 | Referrer URL and title counts for random user | 0.681 | |
| q36 | URL domain counts for random user | 0.342 | |
| q37 | Search phrase counts for random user | 0.224 | |
| q38 | Max/avg event time and histogram for random user | 0.229 | |
| q39 | Count by is-refresh, age, income for random user | 0.320 | |
| q40 | Count distinct users by minute for specific counter | 0.212 | |
| q41 | Date-level distinct user counts for non-refresh hits | 0.228 | |
| q42 | Minute-bucketed page views with offset | 0.225 | |

## Summary

| Metric | ClickHouse OSS | ADX | Winner |
|---|---|---|---|
| **Total query time** | | | |
| **Fastest queries** | | | |
| **Slowest query** | | | |
| **Median query time** | | | |
| **Load time** | | | |
| **Data size** | | | |
