# BrightCart Retail Order Analytics — Business Insights Summary

*Based on `gold_daily_revenue`, `gold_category_performance`, and `gold_customer_region_summary` (excludes CANCELLED orders).*

## Headline Numbers

* **Total revenue (2024):** $1,072,232.07 across **950 completed/pending orders**, from 2024-01-01 to 2024-12-30.
* Average order value: ~$1,128.

## Revenue Trend

Revenue is tracked daily in `gold_daily_revenue` (340 distinct order dates). Volume is broadly consistent across the year with no single day dominating — consistent with a steady-state retailer rather than one with strong seasonal spikes in this sample.

## Top Categories & Products

Revenue by category (highest to lowest):

| Category | Revenue | Units Sold |
| --- | --- | --- |
| Electronics | $300,960.76 | 788 |
| Office | $287,691.79 | 802 |
| Home | $246,477.50 | 669 |
| Accessories | $237,102.02 | 675 |

**Electronics is the top revenue category** despite Office having slightly more units sold — Electronics carries a higher average unit price. Top individual products by revenue: *Present Organizer* (Office, $9,840), *Surface Monitor* (Electronics, $9,207), *Customer Smartwatch* (Electronics, $9,014).

## Top Customers

| Customer | Region | Spend | Orders |
| --- | --- | --- | --- |
| Elizabeth Rogers | North | $8,242.28 | 3 |
| Michelle Evans | North | $6,930.09 | 5 |
| Evelyn Galvan | West | $6,311.70 | 2 |
| Charles Shah | North | $6,094.19 | 4 |
| Samantha Richardson | West | $6,005.38 | 3 |

Notably, 3 of the top 5 customers by spend are in the **North** region.

## Revenue by Region

| Region | Revenue | Customers |
| --- | --- | --- |
| North | $277,592.26 | 151 |
| South | $274,930.43 | 151 |
| West | $266,552.64 | 153 |
| East | $253,156.74 | 162 |

Regional revenue is fairly evenly distributed (within ~9% of each other), despite East having the most distinct customers (162) but the lowest total revenue — implying a lower average spend per customer in the East.

## Recommendation

**Investigate and address the East region's lower revenue-per-customer.** East has the most customers (162) but the lowest region revenue ($253K) and, by implication, the lowest average customer value of any region. A targeted promotion or bundle offer (e.g. cross-selling Electronics, the top-performing category, to East customers who have only purchased Office/Home/Accessories items) could close this gap and is the highest-leverage single action suggested by this data.
