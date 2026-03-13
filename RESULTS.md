# ClickBench Results: ClickHouse OSS vs Azure Data Explorer

## Key Takeaways

- **Overall**: ClickHouse is **2.1x faster** end-to-end (117s vs 250s total query time) on identical hardware (2x E8s_v5, 8 cores / 64 GB each).
- **ADX wins on simple scans and narrow filters** (q00–q11, q19, q36–q42): full-table aggregations, point lookups, and time-range filtered queries run 20–50% faster on ADX.
- **ClickHouse dominates high-cardinality GROUP BY** (q12–q18, q20–q35): queries grouping by millions of distinct strings (SearchPhrase, URL) are 2–10x faster on ClickHouse, driven by its custom hash table implementation with arena-allocated strings and two-level aggregation.
- **Ingestion**: ClickHouse loads 35% faster (4m19s vs 6m38s) and uses 11% less disk (13.71 GB vs 15.41 GB). ADX's larger on-disk footprint is largely due to its 3.6 GB index overhead. ADX achieves a higher compression ratio on raw data (6.0 vs 4.09).

## Environment

| | ClickHouse OSS | Azure Data Explorer |
|---|---|---|
| **SKU** | Standard E8s v5 | Standard E8s v5 |
| **OS** | Ubuntu 22.04 | Windows Server 2022 |
| **Nodes** | 2 | 2 |
| **Cores/node** | 8 | 8 |
| **RAM/node** | 64 GB | 64 GB |

## Dataset

- **Benchmark**: ClickBench (https://benchmark.clickhouse.com/)
- **Table**: hits (105 columns, web-analytics page-view data)
- **Rows**: 96,082,836
- **Source files**: 102 gzipped CSV parts

## Load Performance

| Metric | ClickHouse OSS | ADX |
|---|---|---|
| **Load time** | 4m19s | 6m38s |
| **Data size on disk** | 13.71 GB | 15.41 GB |
| **Index size** | | 3.6 GB |
| **Compression ratio** | 4.09 | 6.0 |

## Query Duration (seconds)

| Query | Description | ClickHouse OSS | ADX | Diff % | Comments |
|---|---|---|---|---|---|
| q00 | COUNT, no filter | 0.193 | 0.098 | +49% | |
| q01 | COUNT with point filter | 0.292 | 0.146 | +50% | |
| q02 | Multiple aggregations (SUM/COUNT/AVG) | 0.233 | 0.182 | +22% | |
| q03 | AVG on high-cardinality column | 0.262 | 0.165 | +37% | |
| q04 | COUNT DISTINCT on high-cardinality column | 0.817 | 0.467 | +43% | |
| q05 | COUNT DISTINCT on high-cardinality string | 1.086 | 0.729 | +33% | |
| q06 | MIN/MAX on date column | 0.203 | 0.096 | +53% | |
| q07 | Low-cardinality GROUP BY with point filter, ORDER BY | 0.215 | 0.134 | +38% | |
| q08 | High-cardinality GROUP BY, DISTINCT count, Top-N | 0.997 | 0.713 | +28% | |
| q09 | High-cardinality GROUP BY, multiple aggregations, Top-N | 1.064 | 0.921 | +13% | |
| q10 | Medium-cardinality string GROUP BY, DISTINCT, Top-N | 0.390 | 0.272 | +30% | |
| q11 | Two-key GROUP BY (int + string), DISTINCT, Top-N | 0.529 | 0.264 | +50% | |
| q12 | High-cardinality string GROUP BY, Top-N | 1.009 | 1.683 | -67% | |
| q13 | High-cardinality string GROUP BY, DISTINCT count, Top-N | 1.557 | 7.734 | -397% | |
| q14 | Two-key GROUP BY (int + high-cardinality string), Top-N | 1.081 | 1.888 | -75% | |
| q15 | High-cardinality GROUP BY, Top-N | 0.893 | 2.889 | -224% | |
| q16 | Two high-cardinality key GROUP BY, Top-N | 2.637 | 5.986 | -127% | |
| q17 | Two high-cardinality key GROUP BY, unordered LIMIT | 1.581 | 5.994 | -279% | |
| q18 | Three-key GROUP BY (high-cardinality + expression), Top-N | 4.618 | 8.064 | -75% | |
| q19 | Point lookup on high-cardinality column | 0.197 | 0.115 | +42% | |
| q20 | Substring filter (LIKE), COUNT | 0.652 | 4.423 | -579% | |
| q21 | Substring filter, high-cardinality GROUP BY, Top-N | 0.328 | 2.935 | -795% | |
| q22 | Two substring filters, GROUP BY, multiple aggregations, Top-N | 0.900 | 2.359 | -162% | |
| q23 | Substring filter, full row SELECT *, ORDER BY, LIMIT | 0.810 | 4.752 | -487% | |
| q24 | Range filter on string, ORDER BY time, LIMIT | 0.499 | 1.437 | -188% | |
| q25 | Range filter on string, ORDER BY string, LIMIT | 0.434 | 0.682 | -57% | |
| q26_single_sort | Range filter on string, ORDER BY time, LIMIT | 0.538 | 1.450 | -170% | |
| q27 | Medium-cardinality GROUP BY, HAVING, Top-N | 0.352 | 4.543 | -1191% | |
| q28 | Regex extraction, GROUP BY, HAVING, Top-N | 15.837 | 18.295 | -16% | |
| q29 | Wide aggregation (90 SUM expressions) | 0.364 | 2.355 | -547% | |
| q30 | Two-key GROUP BY with range filter, Top-N | 0.710 | 2.222 | -213% | |
| q31 | Two high-cardinality key GROUP BY with range filter, Top-N | 1.263 | 4.454 | -253% | |
| q32 | Two high-cardinality key GROUP BY (no filter), Top-N | 4.697 | 12.835 | -173% | |
| q33 | High-cardinality string GROUP BY (URL), Top-N | 4.193 | 9.076 | -116% | |
| q34 | High-cardinality string GROUP BY (URL + constant), Top-N | 4.209 | 9.048 | -115% | |
| q35 | GROUP BY with computed columns, Top-N | 0.626 | 5.475 | -775% | |
| q36 | Time-range + point filter, high-cardinality GROUP BY, Top-N | 0.259 | 0.204 | +21% | |
| q37 | Time-range + point filter, high-cardinality string GROUP BY, Top-N | 0.231 | 0.184 | +20% | |
| q38 | Time-range + multi-predicate filter, GROUP BY, OFFSET pagination | 0.233 | 0.157 | +33% | |
| q39 | Time-range filter, CASE expression, five-key GROUP BY, OFFSET | 0.321 | 0.395 | -23% | |
| q40 | Time-range + IN filter, two-key GROUP BY, OFFSET | 0.211 | 0.122 | +42% | |
| q41 | Time-range + point filter (hash), two-key GROUP BY, OFFSET | 0.213 | 0.114 | +46% | |
| q42 | Time-range filter, time-bucketed GROUP BY, OFFSET | 0.203 | 0.118 | +42% | |

## Summary

| Metric | ClickHouse OSS | ADX |
|---|---|---|
| **Total query time** | 117s | 250s |
| **Load time** | 4m19s | 6m38s |
| **Data size** | 13.71 GB | 15.41 GB |
