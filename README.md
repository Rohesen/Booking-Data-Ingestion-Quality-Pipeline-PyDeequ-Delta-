# 📦 Travel-Booking-Data-Ingestion-Quality-Pipeline-PyDeequ-Delta

## 📋 Project Overview

This project implements an **incremental ETL pipeline** on **Databricks** that:

* Reads **daily incremental booking & customer data** from **Unity Catalog Volumes**
* Runs **data quality checks** using **PyDeequ**
* Transforms and aggregates booking data into a **fact table**
* Maintains customer history using **SCD Type 2** in a **dimension table**
* Writes results to **Delta tables** for analytics and reporting

---

## 🛠 Tech Stack

* **Databricks** (PySpark + Delta Lake)
* **Unity Catalog Volumes**
* **PyDeequ** (data quality checks)
* **DeltaTable MERGE** for upserts
* **Databricks Workflows** for parameterized execution

---

## 📂 Data Sources

Data is stored in Unity Catalog volumes:

```
/Volumes/incremental_load/default/orders_data/booking_data/bookings_<DATE>.csv
/Volumes/incremental_load/default/orders_data/customer_data/customers_<DATE>.csv
```

### **Sample Booking Data Schema**

| Column          | Type    |
| --------------- | ------- |
| booking\_id     | integer |
| customer\_id    | integer |
| booking\_date   | date    |
| amount          | integer |
| booking\_type   | string  |
| quantity        | integer |
| discount        | integer |
| booking\_status | string  |
| hotel\_name     | string  |
| flight\_number  | string  |

### **Sample Customer Data Schema**

| Column            | Type    |
| ----------------- | ------- |
| customer\_id      | integer |
| customer\_name    | string  |
| customer\_address | string  |
| phone\_number     | string  |
| email             | string  |
| valid\_from       | date    |
| valid\_to         | date    |

---

## 🔍 Data Quality Checks (PyDeequ)

Two sets of data quality rules are applied:

### **Booking Data Checks**

* Dataset is not empty
* `booking_id` is unique
* `customer_id` is complete (no nulls)
* `amount` is complete & non-negative
* `quantity` is non-negative
* `discount` is non-negative

### **Customer Data Checks**

* Dataset is not empty
* `customer_id` is unique
* `customer_name`, `customer_address`, and `email` are complete (no nulls)

If **any check fails**, the pipeline stops execution.

---

## 🔄 Transformations & Aggregations

1. Add ingestion timestamp to booking data
2. Join booking data with customer data
3. Calculate `total_cost = amount - discount`
4. Filter out records with `quantity <= 0`
5. Aggregate by `(booking_type, customer_id)`:

   * **Sum of total\_amount**
   * **Sum of total\_quantity**

---

## 📊 Delta Table Outputs

### **Fact Table:** `incremental_load.default.booking_fact`

Stores aggregated booking metrics.

| booking\_type | customer\_id | total\_amount\_sum | total\_quantity\_sum |
| ------------- | ------------ | ------------------ | -------------------- |
| Flight        | 1056         | 779                | 12                   |
| Flight        | 1012         | 1666               | 5                    |
| Hotel         | 1074         | 567                | 3                    |

### **Dimension Table (SCD2):** `incremental_load.default.customer_dim`

Tracks customer changes over time with `valid_from` and `valid_to`.

| customer\_id | customer\_name | customer\_address | phone\_number     | email                                                 | valid\_from | valid\_to  |
| ------------ | -------------- | ----------------- | ----------------- | ----------------------------------------------------- | ----------- | ---------- |
| 1007         | Robert Johnson | 574 Patel Drive…  | 001-927-927-7643… | [danielbrown@west.com](mailto:danielbrown@west.com)   | 2022-09-26  | 9999-12-31 |
| 1093         | Eric Glenn     | 2560 Christian…   | 001-944-838-5681  | [tarabradley@jones.com](mailto:tarabradley@jones.com) | 2023-06-21  | 9999-12-31 |

---

## ⚙️ Execution

The pipeline is parameterized by `arrival_date`:

```python
date_str = dbutils.widgets.get("arrival_date")
```

In **Databricks Jobs**, pass the date as a job parameter:

```
arrival_date = 2024-07-26
```

---

## 🚀 How to Run

1. Upload the PySpark notebook/script to Databricks
2. Create a **Databricks Workflow**:

   * Task type: Notebook or Python Script
   * Parameter: `arrival_date` (YYYY-MM-DD)
3. Run the job or schedule it for daily execution
4. Verify data in Delta tables:

```sql
SELECT * FROM incremental_load.default.booking_fact;
SELECT * FROM incremental_load.default.customer_dim;
```

---

## 📌 Key Learnings

* **PyDeequ** integrates well with Databricks for automated DQ checks
* **SCD2 with Delta MERGE** ensures historical tracking of dimension data
* **Unity Catalog Volumes** simplify external file ingestion
* Parameterized workflows allow flexible daily incremental loads

---
