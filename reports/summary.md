# Week 2 Summary — ETL + EDA

## Key findings
- **Number of rows (orders)**: 50 orders
- **Number of users**: 10 users
- **Number of countries**: 3 (SA, AE, KW)
- **Time span**: 2025-12-01-2025-12-20 (19 days)
- **Paid orders**: 45 orders is `paid` with rate `0.9`
- **Missing amount rate**: 0.14
- **Missing quantity rate**: 0.08
- **Amount outliers**: 45 order amount is `outlier` with rate `0.02`

## Definitions
- **Revenue** = sum(`amount`) fro orders orders.
- **Status processed** = normalized `status` field from raw status column
- **Refund** = order where `status_proccess == "refund"` after status normalization.
- **Refund rate** = number of refunded orders over total orders.
- **Outlier (amount)** = values outside the IQR-based bounds
- **Missingness flags**:
  - `amount__isna` = 1 if amount is missing, else 0
  - `quantity__isna` = 1 if quantity is missing, else 0
- **amount winsor** = order amount after winsorization that values below `lo` and above `hi`  using the IQR method.
- **amount__is_outlier** = 1 if amount is outlier, else 0 based if `amount` falls outside the IQR method


## Data quality caveats
- **Missingness**: a small number of orders have missing `amount` with rate of 0.14, `quantity` with rate of 0.08 and `signup_date` with rate of 0.14; missingness is low overall may not have big effection.
- **Duplicates**: no duplicate `order_id`.
- **Outliers**: one order (`amount = 200`) is an outlier using the IQR method. After winsorization, this value changed in `amount_winsor = 158`.


## EDA questions
- What is the refund rate by country?
![Alt text](..\reports\figures\refund_by_country.png)

- Who are the top 10 Users by Revenue?
![Alt text](..\reports\figures\top_users_by_revenue.png)

- Which countries has the higest revenue?
![Alt text](..\reports\figures\revenue_by_country.png)

- which day has highest revenue?
![Alt text](..\reports\figures\revenue_by_days.png)

- which days has many orders volume and days has less?
![Alt text](..\reports\figures\orders_by_days.png)

- which hours has many orders volume and hours has less?
![Alt text](..\reports\figures\orders_by_hours.png)
