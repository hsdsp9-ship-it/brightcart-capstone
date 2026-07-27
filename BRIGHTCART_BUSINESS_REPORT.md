# BrightCart Retail Analytics — 2024 Business Performance Report

**Prepared by:** Data Engineering Team  
**Reporting Period:** January 1, 2024 — December 30, 2024  
**Data Source:** Gold-layer Delta tables (`gold_daily_revenue`, `gold_category_performance`, `gold_customer_region_summary`)  
**Pipeline:** BrightCart Retail Order Analytics — Databricks Capstone Project  
**Note:** All figures exclude CANCELLED orders. Only COMPLETED and PENDING orders are counted.

---

## Executive Summary

BrightCart generated **$1,072,232.07 in total revenue** across **950 orders** and **2,934 units sold** in 2024. The business operates with a healthy average order value of approximately **$1,128**, and revenue is remarkably consistent across all four quarters — indicating a stable, mature customer base rather than one with strong seasonal dependency.

Electronics is the clear revenue leader among product categories, driven by higher unit prices. The North region leads across all revenue metrics, while the East region presents the single most significant growth opportunity: it has the most customers of any region but generates the lowest total revenue.

---

## 1. Total Revenue & Key Performance Indicators

| KPI | Value |
| --- | --- |
| **Total Revenue (2024)** | **$1,072,232.07** |
| Total Orders | 950 |
| Total Units Sold | 2,934 |
| Average Order Value | ~$1,128 |
| Reporting Period | Jan 1, 2024 — Dec 30, 2024 |
| Active Order Dates | 340 distinct days |

**Takeaway:** BrightCart processes roughly 2–3 orders per trading day on average. There are no data gaps suggesting operational outages, and the steady spread across 340 days confirms consistent demand.

---

## 2. Revenue Trend Over Time

Revenue is tracked at daily granularity in `gold_daily_revenue`. The 7-day rolling average shows a broadly flat trend with no dominant seasonal spikes or troughs across the full year.

**Monthly revenue breakdown:**

| Month | Est. Revenue Range |
| --- | --- |
| Jan – Mar | Steady baseline |
| Apr – Jun | Consistent, slight positive drift |
| Jul – Sep | Stable mid-year |
| Oct – Dec | Consistent, year-end steady |

> See notebook `07_business_analytics_report` — Section 2 for the full daily and monthly chart.

**Observation:** Revenue does not exhibit a traditional retail "holiday spike" in Q4. This may reflect B2B purchasing patterns or the synthetic nature of the dataset. A real-world recommendation would be to layer in promotional calendar data to identify whether marketing campaigns correlate with revenue upticks.

**Recommendation:** Introduce at least one structured promotional event per quarter (e.g., mid-year clearance, back-to-school, year-end bundle deals) to create intentional revenue peaks and test price elasticity.

---

## 3. Revenue by Product Category

| Category | Revenue | % of Total | Units Sold | Orders | Avg Rev / Order |
| --- | --- | --- | --- | --- | --- |
| **Electronics** | **$300,960.76** | **28.1%** | 788 | — | Highest |
| Office | $287,691.79 | 26.8% | 802 | — | High |
| Home | $246,477.50 | 23.0% | 669 | — | Medium |
| Accessories | $237,102.02 | 22.1% | 675 | — | Lowest |

**Key Finding — Electronics vs. Office:**  
Electronics generates more revenue than Office despite selling fewer units (788 vs. 802). This is entirely explained by unit price: Electronics items carry a significantly higher price point, meaning each unit sold contributes more to the top line.

**Key Finding — Accessories underperformance:**  
Accessories has both the lowest revenue and a revenue-per-order that is well below Electronics. This suggests Accessories are being purchased as standalone low-value items rather than as add-ons to higher-value Electronics purchases.

**Recommendations:**
- Protect Electronics gross margin — avoid deep discounting.
- Bundle Electronics + Accessories at a small discount to lift Accessories revenue without eroding Electronics margin.
- Investigate whether Office items have higher return/cancellation rates than Electronics (not visible in this dataset but worth checking in `silver_enriched_orders`).

---

## 4. Top 10 Best-Selling Products

| Rank | Product | Category | Revenue | Units Sold |
| --- | --- | --- | --- | --- |
| 1 | Present Organizer | Office | ~$9,840 | — |
| 2 | Surface Monitor | Electronics | ~$9,207 | — |
| 3 | Customer Smartwatch | Electronics | ~$9,014 | — |
| 4–10 | Various | Mixed | $6,000–$9,000 | — |

> Full ranked table with exact figures available in notebook Section 4.

**Key Finding:**  
The top product (Present Organizer, Office) outsells the #2 Electronics product by revenue despite Office items generally carrying lower prices. This anomaly suggests the Present Organizer either has an unusually high price or very high volume — worth investigating to determine if it's a data artefact or a genuine bestseller.

**Recommendations:**
- Ensure the top 3 products are never out of stock — they disproportionately drive revenue.
- Use the top-10 product list as the foundation for a "BrightCart Bestsellers" homepage section or email campaign.
- Review pricing on the bottom half of the product catalogue to ensure no products are being sold below margin.

---

## 5. Revenue by Region

| Region | Total Revenue | Unique Customers | Rev / Customer | Rank |
| --- | --- | --- | --- | --- |
| **North** | **$277,592.26** | 151 | ~$1,839 | 1st |
| South | $274,930.43 | 151 | ~$1,821 | 2nd |
| West | $266,552.64 | 153 | ~$1,743 | 3rd |
| **East** | **$253,156.74** | **162** | **~$1,563** | **4th** |
| **Total** | **$1,072,231.07** | **617** | **~$1,738** | — |

**Key Finding — East Region Efficiency Gap:**  
East has 162 customers — more than any other region — but the lowest total revenue ($253K) and the lowest revenue per customer (~$1,563). North, by contrast, achieves $1,839 per customer with the same number of customers as South (151). The gap between East and North revenue-per-customer is **$276 per customer**.

At 162 East customers, closing just 50% of that gap would generate an additional **~$22,000 in annual revenue** at zero customer acquisition cost.

**Key Finding — Regional parity at the aggregate level:**  
All four regions are within 9% of each other in total revenue ($253K–$278K). This means BrightCart has no single-region dependency risk, but it also means there are no breakout growth regions either — every region has room to grow.

**Recommendations:**
1. **Priority action:** Run a targeted Electronics cross-sell campaign for East customers who have only purchased Office/Home/Accessories items. Electronics is the highest-revenue category and is currently underrepresented in East.
2. Use North as a benchmark for customer engagement. Analyse what North customers buy differently (likely more Electronics and higher-AOV products) and replicate that product mix promotion in East.
3. Consider a regional loyalty tier that activates when a customer's annual spend crosses a threshold, giving West and South customers an incentive to reach North-level spending.

---

## 6. Top Customers by Spend

| Rank | Customer | Region | Total Spend | Orders | Units |
| --- | --- | --- | --- | --- | --- |
| 1 | Elizabeth Rogers | North | $8,242.28 | 3 | — |
| 2 | Michelle Evans | North | $6,930.09 | 5 | — |
| 3 | Evelyn Galvan | West | $6,311.70 | 2 | — |
| 4 | Charles Shah | North | $6,094.19 | 4 | — |
| 5 | Samantha Richardson | West | $6,005.38 | 3 | — |
| 6–10 | Various | Mixed | $4,000–$5,800 | — | — |

> Full top-10 table with units purchased available in notebook Section 6.

**Key Finding — High value, low frequency:**  
The top customer (Elizabeth Rogers) spent $8,242 in just 3 orders — an average of **$2,747 per order**. Evelyn Galvan (rank 3) spent $6,312 in only 2 orders — **$3,156 per order**. These customers are not frequent shoppers; they are high-AOV buyers who purchase infrequently.

**Key Finding — North dominates the top-5:**  
3 of the 5 highest-spending customers are in the North region, which reinforces North's aggregate revenue leadership and indicates a concentration of high-value individual buyers in that region.

**Recommendations:**
1. Introduce a **VIP / loyalty programme** that rewards cumulative annual spend rather than order frequency. This targets the "high-AOV, low-frequency" profile of top customers.
2. Identify what the top-10 customers purchased (product categories and specific SKUs) and use this as a "high-value customer product affinity" model for personalised recommendations.
3. Flag any top-10 customer who has not placed an order in the last 90 days as "at-risk" and trigger a re-engagement email with a personalised offer.

---

## 7. Region × Category Opportunity Matrix

Cross-referencing region and category from `silver_enriched_orders`:

| | Electronics | Office | Home | Accessories |
| --- | --- | --- | --- | --- |
| **North** | Strongest | Strong | Moderate | Moderate |
| **South** | Strong | Strong | Moderate | Moderate |
| **West** | Moderate | Moderate | Strong | Moderate |
| **East** | **Weak** | Moderate | Moderate | Moderate |

> Exact revenue figures per cell are rendered as a colour heatmap in notebook Section 8.

**Key Finding:** Electronics revenue from the East region is the most visible white-space opportunity in the entire dataset. East customers exist and are buying (from Office, Home, Accessories), but Electronics penetration in East is below all other regions.

---

## 8. Summary of Strategic Recommendations

| Priority | Recommendation | Expected Impact | Effort |
| --- | --- | --- | --- |
| **HIGH** | Cross-sell Electronics to East region customers | +$20K–$25K annual revenue (est.) | Low |
| **HIGH** | VIP/loyalty programme for top customers (high AOV, low frequency) | Increase order frequency by 1 order/year per top-50 customer | Medium |
| **MEDIUM** | Bundle Electronics + Accessories with 5–10% discount | Lift Accessories revenue, minimal margin erosion | Low |
| **MEDIUM** | Introduce quarterly promotional events | Revenue peaks, test price elasticity | Medium |
| **LOW** | Replicate North engagement model in West and South | Gradual rev/customer improvement across 3 regions | High |

---

## 9. Data Quality Notes

- All figures are derived from COMPLETED and PENDING orders. CANCELLED orders (present in `silver_enriched_orders` with `is_cancelled = true`) are excluded from all revenue calculations.
- The dataset covers 1,000 synthetic orders across 1,000 customers and 1,000 products, generated by `00_data_generation`.
- No returns or partial refunds are modelled in the current pipeline. Adding a `returns.csv` (stretch goal) would reduce net revenue figures.
- Revenue figures represent gross order value (`quantity × unit_price`) with no discounts, taxes, or shipping modelled.

---

## 10. Appendix — Report Artefacts

| Artefact | Location |
| --- | --- |
| Analysis notebook (charts + insights) | `notebooks/07_business_analytics_report` |
| Self-contained HTML report | `BRIGHTCART_REPORT_2024.html` (generated by notebook) |
| This markdown report | `BRIGHTCART_BUSINESS_REPORT.md` |
| Raw business insights (brief) | `BUSINESS_INSIGHTS.md` |
| Gold Delta tables | `harpalsingh.brightcart.gold_*` |

---

*BrightCart Retail Analytics Pipeline — Databricks Capstone Project*  
*All data is synthetic and generated for training purposes.*
