# ClickBench Results: ClickHouse OSS vs Azure Data Explorer

## Key Takeaways

- **Overall**: ClickHouse is **1.9x faster** end-to-end (57.9s vs 108.2s total query time) on identical hardware (2x Standard E8s v5, 8 cores / 64 GB each).
- **ADX wins 19 of 43 queries**: simple scans, narrow filters, full-table aggregations, point lookups, and time-range filtered queries.
- **ClickHouse wins 20 of 43 queries**: high-cardinality GROUP BY, substring filters, wide aggregations, and unfiltered joins.
- **4 ties**: within 10%, effectively equivalent.
- **Ingestion**: ClickHouse loads 26% faster (4m19s vs 5m51s). ADX uses 7% less disk (12.79 GB vs 13.71 GB).

## Environment

| | ClickHouse OSS | Azure Data Explorer |
|---|---|---|
| **SKU** | Standard E8s v5 | Standard E8s v5 |
| **OS** | Ubuntu 22.04 | Windows Server 2022 |
| **Nodes** | 2 | 2 |
| **Cores/node** | 8 | 8 |
| **RAM/node** | 64 GB | 64 GB |

## ADX Optimizations

- **Inverted index disabled**: The default inverted index policy was turned off because it slowed down ingestion and was never utilized by any of the benchmark queries.
- **Shuffle strategy**: `hint.strategy=shuffle` was manually added to KQL queries with high-cardinality aggregations to distribute GROUP BY work across nodes.
- **Memory limits increased**: `set maxmemoryconsumptionperiterator=17179869184` was applied to all queries to accommodate high-cardinality aggregations that exceed default memory limits.

## Dataset

- **Benchmark**: ClickBench (https://benchmark.clickhouse.com/)
- **Table**: hits (105 columns, web-analytics page-view data)
- **Rows**: 96,082,836
- **Source files**: 102 gzipped CSV parts
- **Uncompressed size**: 93.74 GB

## Load Performance

| Metric | ClickHouse OSS | ADX |
|---|---|---|
| **Load time** | 4m19s | 5m51s |
| **Data size on disk** | 13.71 GB | 12.79 GB |
| **Index size** | 4.5 MB | 190 MB |
| **Compression ratio** | 6.84 | 7.33 |

## Query Duration (seconds)

🟢 = ADX more than 10% faster. ⚪ = Tie (within 10%). 🔴 = ADX more than 10% slower than ClickHouse.

| Query | Description | ClickHouse OSS | ADX | Diff % | Comments |
|---|---|---|---|---|---|
| q00 | COUNT, no filter | 0.193 | 0.091 | +53% | 🟢 |
| q01 | COUNT with point filter | 0.292 | 0.122 | +58% | 🟢 |
| q02 | Multiple aggregations (SUM/COUNT/AVG) | 0.233 | 0.185 | +21% | 🟢 |
| q03 | AVG on high-cardinality column | 0.262 | 0.157 | +40% | 🟢 |
| q04 | COUNT DISTINCT on high-cardinality column | 0.817 | 0.303 | +63% | 🟢 |
| q05 | COUNT DISTINCT on high-cardinality string | 1.086 | 0.374 | +66% | 🟢 |
| q06 | MIN/MAX on date column | 0.203 | 0.092 | +55% | 🟢 |
| q07 | Low-cardinality GROUP BY with point filter, ORDER BY | 0.215 | 0.126 | +41% | 🟢 |
| q08 | High-cardinality GROUP BY, DISTINCT count, Top-N | 0.997 | 0.761 | +24% | 🟢 |
| q09 | High-cardinality GROUP BY, multiple aggregations, Top-N | 1.064 | 1.021 | +4% | ⚪ |
| q10 | Medium-cardinality string GROUP BY, DISTINCT, Top-N | 0.390 | 0.268 | +31% | 🟢 |
| q11 | Two-key GROUP BY (int + string), DISTINCT, Top-N | 0.529 | 0.242 | +54% | 🟢 |
| q12 | High-cardinality string GROUP BY, Top-N | 1.009 | 1.874 | -86% | 🔴 |
| q13 | High-cardinality string GROUP BY, DISTINCT count, Top-N | 1.557 | 7.779 | -400% | 🔴 |
| q14 | Two-key GROUP BY (int + high-cardinality string), Top-N | 1.081 | 1.981 | -83% | 🔴 |
| q15 | High-cardinality GROUP BY, Top-N | 0.893 | 3.045 | -241% | 🔴 |
| q16 | Two high-cardinality key GROUP BY, Top-N | 2.637 | 6.089 | -131% | 🔴 |
| q17 | Two high-cardinality key GROUP BY, unordered LIMIT | 1.581 | 5.371 | -240% | 🔴 |
| q18 | Three-key GROUP BY (high-cardinality + expression), Top-N | 4.618 | 8.328 | -80% | 🔴 |
| q19 | Point lookup on high-cardinality column | 0.197 | 0.113 | +43% | 🟢 |
| q20 | Substring filter (LIKE), COUNT | 0.652 | 0.960 | -47% | 🔴 |
| q21 | Substring filter, high-cardinality GROUP BY, Top-N | 0.328 | 0.970 | -196% | 🔴 |
| q22 | Two substring filters, GROUP BY, multiple aggregations, Top-N | 0.900 | 0.896 | +0% | ⚪ |
| q23 | Substring filter, full row SELECT *, ORDER BY, LIMIT | 0.810 | 2.446 | -202% | 🔴 |
| q24 | Non-empty string filter, ORDER BY time, LIMIT | 0.499 | 0.992 | -99% | 🔴 |
| q25 | Non-empty string filter, ORDER BY string, LIMIT | 0.434 | 0.345 | +21% | 🟢 |
| q26 | Non-empty string filter, ORDER BY time + string, LIMIT | 0.538 | 0.990 | -84% | 🔴 |
| q27 | Medium-cardinality GROUP BY, HAVING, Top-N | 0.352 | 2.882 | -719% | 🔴 |
| q28 | Regex extraction, GROUP BY, HAVING, Top-N | 15.837 | 16.859 | -6% | ⚪ |
| q29 | Wide aggregation (90 SUM expressions) | 0.364 | 1.722 | -373% | 🔴 |
| q30 | Two-key GROUP BY with range filter, Top-N | 0.710 | 1.502 | -112% | 🔴 |
| q31 | Two high-cardinality key GROUP BY with range filter, Top-N | 1.263 | 1.888 | -49% | 🔴 |
| q32 | Two high-cardinality key GROUP BY (no filter), Top-N | 4.697 | 13.462 | -187% | 🔴 |
| q33 | High-cardinality string GROUP BY (URL), Top-N | 4.193 | 8.457 | -102% | 🔴 |
| q34 | High-cardinality string GROUP BY (URL + constant), Top-N | 4.209 | 8.621 | -105% | 🔴 |
| q35 | GROUP BY with computed columns, Top-N | 0.626 | 5.732 | -816% | 🔴 |
| q36 | Time-range + point filter, high-cardinality GROUP BY, Top-N | 0.259 | 0.155 | +40% | 🟢 |
| q37 | Time-range + point filter, high-cardinality string GROUP BY, Top-N | 0.231 | 0.150 | +35% | 🟢 |
| q38 | Time-range + multi-predicate filter, GROUP BY, OFFSET pagination | 0.233 | 0.128 | +45% | 🟢 |
| q39 | Time-range filter, CASE expression, five-key GROUP BY, OFFSET | 0.321 | 0.319 | +1% | ⚪ |
| q40 | Time-range + IN filter, two-key GROUP BY, OFFSET | 0.211 | 0.111 | +47% | 🟢 |
| q41 | Time-range + point filter (hash), two-key GROUP BY, OFFSET | 0.213 | 0.110 | +48% | 🟢 |
| q42 | Time-range filter, time-bucketed GROUP BY, OFFSET | 0.203 | 0.112 | +45% | 🟢 |

## Summary

| Metric | ClickHouse OSS | ADX |
|---|---|---|
| **Total query time** | 57.9s | 108.2s |
| **Load time** | 4m19s | 5m51s |
| **Data size on disk** | 13.71 GB | 12.79 GB |

## Action Items

1. ~~Fix `dcount` performance bugs~~ (fixed but not yet deployed as of 2026-03-15) - should improve q04, q05, q08-q11, q13.
2. Consider adding common subexpression optimization - should significantly improve q29 and q35.
3. Consider implementing two-level hashing - would narrow the gap on high-cardinality GROUP BY queries (q12-q18, q32-q34).

## Appendix: Query Definitions (ADX time : ClickHouse time)

### q00 (0.091s : 0.193s) 🟢

**SQL:**
```sql
SELECT COUNT(*) FROM hits
```

**KQL:**
```kql
hits
| count
```

### q01 (0.122s : 0.292s) 🟢

**SQL:**
```sql
SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0
```

**KQL:**
```kql
hits
| where AdvEngineID != 0
| count
```

### q02 (0.185s : 0.233s) 🟢

**SQL:**
```sql
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits
```

**KQL:**
```kql
hits
| summarize sum(AdvEngineID), count(), avg(ResolutionWidth)
```

### q03 (0.157s : 0.262s) 🟢

**SQL:**
```sql
SELECT AVG(UserID) FROM hits
```

**KQL:**
```kql
hits
| summarize avg(UserID)
```

### q04 (0.303s : 0.817s) 🟢

**SQL:**
```sql
SELECT COUNT(DISTINCT UserID) FROM hits
```

**KQL:**
```kql
hits
| summarize dcount(UserID)
```

### q05 (0.374s : 1.086s) 🟢

**SQL:**
```sql
SELECT COUNT(DISTINCT SearchPhrase) FROM hits
```

**KQL:**
```kql
hits
| summarize dcount(SearchPhrase)
```

### q06 (0.092s : 0.203s) 🟢

**SQL:**
```sql
SELECT MIN(EventDate), MAX(EventDate) FROM hits
```

**KQL:**
```kql
hits
| summarize min(EventDate), max(EventDate)
```

### q07 (0.126s : 0.215s) 🟢

**SQL:**
```sql
SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC
```

**KQL:**
```kql
hits
| where AdvEngineID != 0
| summarize cnt = count() by AdvEngineID
| order by cnt desc
```

### q08 (0.761s : 0.997s) 🟢

**SQL:**
```sql
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10
```

**KQL:**
```kql
hits
| summarize u = dcount(UserID) by RegionID
| top 10 by u desc
```

### q09 (1.021s : 1.064s) ⚪

**SQL:**
```sql
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| summarize sum(AdvEngineID), c = count(), avg(ResolutionWidth), dcount(UserID) by RegionID
| top 10 by c desc
```

### q10 (0.268s : 0.390s) 🟢

**SQL:**
```sql
SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10
```

**KQL:**
```kql
hits
| where MobilePhoneModel != ""
| summarize u = dcount(UserID) by MobilePhoneModel
| top 10 by u desc
```

### q11 (0.242s : 0.529s) 🟢

**SQL:**
```sql
SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10
```

**KQL:**
```kql
hits
| where MobilePhoneModel != ""
| summarize u = dcount(UserID) by MobilePhone, MobilePhoneModel
| top 10 by u desc
```

### q12 (1.874s : 1.009s) 🔴

**SQL:**
```sql
SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| where SearchPhrase != ""
| summarize hint.strategy=shuffle c = count() by SearchPhrase
| top 10 by c desc
```

### q13 (7.779s : 1.557s) 🔴

**SQL:**
```sql
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10
```

**KQL:**
```kql
hits
| where SearchPhrase != ""
| summarize hint.strategy=shuffle u = dcount(UserID) by SearchPhrase
| top 10 by u desc
```

### q14 (1.981s : 1.081s) 🔴

**SQL:**
```sql
SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| where SearchPhrase != ""
| summarize hint.strategy=shuffle c = count() by SearchEngineID, SearchPhrase
| top 10 by c desc
```

### q15 (3.045s : 0.893s) 🔴

**SQL:**
```sql
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10
```

**KQL:**
```kql
hits
| summarize hint.strategy=shuffle c = count() by UserID
| top 10 by c desc
```

### q16 (6.089s : 2.637s) 🔴

**SQL:**
```sql
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
```

**KQL:**
```kql
hits
| summarize hint.strategy=shuffle c = count() by UserID, SearchPhrase
| top 10 by c desc
```

### q17 (5.371s : 1.581s) 🔴

**SQL:**
```sql
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10
```

**KQL:**
```kql
hits
| summarize hint.strategy=shuffle c = count() by UserID, SearchPhrase
| take 10
```

### q18 (8.328s : 4.618s) 🔴

**SQL:**
```sql
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
```

**KQL:**
```kql
hits
| summarize hint.strategy=shuffle c = count() by UserID, m = datetime_part("minute", EventTime), SearchPhrase
| top 10 by c desc
```

### q19 (0.113s : 0.197s) 🟢

**SQL:**
```sql
SELECT UserID FROM hits WHERE UserID = 435090932899640449
```

**KQL:**
```kql
hits
| where UserID == 435090932899640449
```

### q20 (0.960s : 0.652s) 🔴

**SQL:**
```sql
SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'
```

**KQL:**
```kql
hits
| where URL contains_cs "google"
| count
```

### q21 (0.970s : 0.328s) 🔴

**SQL:**
```sql
SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| where URL contains_cs "google" and SearchPhrase != ""
| summarize hint.strategy=shuffle min(URL), c = count() by SearchPhrase
| top 10 by c desc
```

### q22 (0.896s : 0.900s) ⚪

**SQL:**
```sql
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| where Title contains_cs "Google" and not(URL contains_cs ".google.") and SearchPhrase != ""
| summarize hint.strategy=shuffle min(URL), min(Title), c = count(), dcount(UserID) by SearchPhrase
| top 10 by c desc
```

### q23 (2.446s : 0.810s) 🔴

**SQL:**
```sql
SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10
```

**KQL:**
```kql
hits
| where URL contains_cs "google"
| order by EventTime asc
| take 10
```

### q24 (0.992s : 0.499s) 🔴

**SQL:**
```sql
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10
```

**KQL:**
```kql
hits
| where SearchPhrase != ""
| order by EventTime asc
| take 10
```

### q25 (0.345s : 0.434s) 🟢

**SQL:**
```sql
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10
```

**KQL:**
```kql
hits
| where SearchPhrase != ""
| order by SearchPhrase asc
| take 10
```

### q26 (0.990s : 0.538s) 🔴

**SQL:**
```sql
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10
```

**KQL:**
```kql
hits
| where SearchPhrase != ""
| order by EventTime asc, SearchPhrase asc
| take 10
```

### q27 (2.882s : 0.352s) 🔴

**SQL:**
```sql
SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
```

**KQL:**
```kql
hits
| where URL != ""
| summarize l = avg(strlen(URL)), c = count() by CounterID
| where c > 100000
| top 25 by l desc
```

### q28 (16.859s : 15.837s) ⚪

**SQL:**
```sql
SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
```

**KQL:**
```kql
hits
| where Referer != ""
| extend key = extract("^https?://(?:www\\.)?([^/]+)/.*$", 1, Referer)
| summarize l = avg(strlen(Referer)), c = count(), min(Referer) by key
| where c > 100000
| top 25 by l desc
```

### q29 (1.722s : 0.364s) 🔴

**SQL:**
```sql
SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ..., SUM(ResolutionWidth + 89) FROM hits
```

**KQL:**
```kql
hits
| summarize sum(ResolutionWidth), sum(ResolutionWidth + 1), ..., sum(ResolutionWidth + 89)
```

### q30 (1.502s : 0.710s) 🔴

**SQL:**
```sql
SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| where SearchPhrase != ""
| summarize hint.strategy=shuffle c = count(), sum(IsRefresh), avg(ResolutionWidth) by SearchEngineID, ClientIP
| top 10 by c desc
```

### q31 (1.888s : 1.263s) 🔴

**SQL:**
```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| where SearchPhrase != ""
| summarize hint.strategy=shuffle c = count(), sum(IsRefresh), avg(ResolutionWidth) by WatchID, ClientIP
| top 10 by c desc
```

### q32 (13.462s : 4.697s) 🔴

**SQL:**
```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| summarize hint.strategy=shuffle c = count(), sum(IsRefresh), avg(ResolutionWidth) by WatchID, ClientIP
| top 10 by c desc
```

### q33 (8.457s : 4.193s) 🔴

**SQL:**
```sql
SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| summarize hint.strategy=shuffle c = count() by URL
| top 10 by c desc
```

### q34 (8.621s : 4.209s) 🔴

**SQL:**
```sql
SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| summarize hint.strategy=shuffle c = count() by Column1 = 1, URL
| top 10 by c desc
```

### q35 (5.732s : 0.626s) 🔴

**SQL:**
```sql
SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10
```

**KQL:**
```kql
hits
| extend ClientIP1 = ClientIP - 1, ClientIP2 = ClientIP - 2, ClientIP3 = ClientIP - 3
| summarize hint.strategy=shuffle c = count() by ClientIP, ClientIP1, ClientIP2, ClientIP3
| top 10 by c desc
```

### q36 (0.155s : 0.259s) 🟢

**SQL:**
```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10
```

**KQL:**
```kql
hits
| where CounterID == 62 and EventDate >= datetime(2013-07-01) and EventDate <= datetime(2013-07-31) and DontCountHits == 0 and IsRefresh == 0 and URL != ""
| summarize hint.strategy=shuffle PageViews = count() by URL
| top 10 by PageViews desc
```

### q37 (0.150s : 0.231s) 🟢

**SQL:**
```sql
SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10
```

**KQL:**
```kql
hits
| where CounterID == 62 and EventDate >= datetime(2013-07-01) and EventDate <= datetime(2013-07-31) and DontCountHits == 0 and IsRefresh == 0 and Title != ""
| summarize hint.strategy=shuffle PageViews = count() by Title
| top 10 by PageViews desc
```

### q38 (0.128s : 0.233s) 🟢

**SQL:**
```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
```

**KQL:**
```kql
hits
| where CounterID == 62 and EventDate >= datetime(2013-07-01) and EventDate <= datetime(2013-07-31) and IsRefresh == 0 and IsLink != 0 and IsDownload == 0
| summarize hint.strategy=shuffle PageViews = count() by URL
| order by PageViews desc
| serialize rn = row_number()
| where rn > 1000 and rn <= 1010
| project-away rn
```

### q39 (0.319s : 0.321s) ⚪

**SQL:**
```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
```

**KQL:**
```kql
hits
| where CounterID == 62 and EventDate >= datetime(2013-07-01) and EventDate <= datetime(2013-07-31) and IsRefresh == 0
| extend Src = iff(SearchEngineID == 0 and AdvEngineID == 0, Referer, ""), Dst = URL
| summarize hint.strategy=shuffle PageViews = count() by TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst
| order by PageViews desc
| serialize rn = row_number()
| where rn > 1000 and rn <= 1010
| project-away rn
```

### q40 (0.111s : 0.211s) 🟢

**SQL:**
```sql
SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100
```

**KQL:**
```kql
hits
| where CounterID == 62 and EventDate >= datetime(2013-07-01) and EventDate <= datetime(2013-07-31) and IsRefresh == 0 and TraficSourceID in (-1, 6) and RefererHash == 3594120000172545465
| summarize PageViews = count() by URLHash, EventDate
| order by PageViews desc
| serialize rn = row_number()
| where rn > 100 and rn <= 110
| project-away rn
```

### q41 (0.110s : 0.213s) 🟢

**SQL:**
```sql
SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000
```

**KQL:**
```kql
hits
| where CounterID == 62 and EventDate >= datetime(2013-07-01) and EventDate <= datetime(2013-07-31) and IsRefresh == 0 and DontCountHits == 0 and URLHash == 2868770270353813622
| summarize PageViews = count() by WindowClientWidth, WindowClientHeight
| order by PageViews desc
| serialize rn = row_number()
| where rn > 10000 and rn <= 10010
| project-away rn
```

### q42 (0.112s : 0.203s) 🟢

**SQL:**
```sql
SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000
```

**KQL:**
```kql
hits
| where CounterID == 62 and EventDate >= datetime(2013-07-14) and EventDate <= datetime(2013-07-15) and IsRefresh == 0 and DontCountHits == 0
| summarize PageViews = count() by M = bin(EventTime, 1m)
| order by M asc
| serialize rn = row_number()
| where rn > 1000 and rn <= 1010
| project-away rn
```
