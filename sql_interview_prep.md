# SQL Interview Preparation — Master Reference
### (Amazon, Google, Meta, Microsoft, Apple, Netflix, Uber, LinkedIn, Databricks, etc.)

Organized by category. Each question has a direct solution. Use this as a revision sheet before interviews.

---

# 1. JOIN Scenarios

### 1.1 Inner vs Left vs Right vs Full — Customers with/without Orders
```sql
-- Customers who never ordered
SELECT c.customer_id, c.name
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;

-- Orders with no matching customer (orphan records)
SELECT o.*
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Full outer join to find mismatches both ways
SELECT c.customer_id, o.order_id
FROM customers c
FULL OUTER JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL OR o.order_id IS NULL;
```

### 1.2 Self Join — Employees with Same Manager
```sql
SELECT e1.name AS emp1, e2.name AS emp2, e1.manager_id
FROM employees e1
JOIN employees e2
  ON e1.manager_id = e2.manager_id
 AND e1.id < e2.id;
```

### 1.3 Employees Earning More Than Their Manager
```sql
SELECT e.name AS employee_name
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE e.salary > m.salary;
```

### 1.4 Non-Equi Join — Salary Grades / Bucketing
```sql
SELECT e.name, g.grade
FROM employees e
JOIN salary_grades g
  ON e.salary BETWEEN g.min_salary AND g.max_salary;
```

### 1.5 Cross Join — Generate All Combinations (Date x Store Matrix)
```sql
SELECT d.date, s.store_id
FROM date_dim d
CROSS JOIN stores s;
```

### 1.6 Anti-Join Pattern (NOT EXISTS is safest with NULLs)
```sql
SELECT c.customer_id
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);
```

### 1.7 Semi-Join Pattern (Customers Who Placed at Least 1 Order)
```sql
SELECT c.customer_id
FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);
```

---

# 2. Aggregation, GROUP BY, HAVING

### 2.1 Departments with More Than 5 Employees
```sql
SELECT department_id, COUNT(*) AS emp_count
FROM employees
GROUP BY department_id
HAVING COUNT(*) > 5;
```

### 2.2 WHERE vs HAVING — Total Sales Per Region Above Threshold
```sql
SELECT region, SUM(sales) AS total_sales
FROM sales
WHERE sale_date >= '2026-01-01'      -- filters rows before aggregation
GROUP BY region
HAVING SUM(sales) > 100000;          -- filters after aggregation
```

### 2.3 Multiple Aggregates in One Query
```sql
SELECT department_id,
       COUNT(*) AS headcount,
       AVG(salary) AS avg_salary,
       MIN(salary) AS min_salary,
       MAX(salary) AS max_salary,
       SUM(salary) AS total_payroll
FROM employees
GROUP BY department_id;
```

### 2.4 GROUP BY with ROLLUP (Subtotals + Grand Total)
```sql
SELECT department, job_title, SUM(salary) AS total_salary
FROM employees
GROUP BY ROLLUP (department, job_title);
```

### 2.5 GROUP BY with CUBE (All Combinations of Subtotals)
```sql
SELECT department, job_title, SUM(salary) AS total_salary
FROM employees
GROUP BY CUBE (department, job_title);
```

### 2.6 Conditional Aggregation (COUNT IF style)
```sql
SELECT department,
       COUNT(CASE WHEN gender = 'F' THEN 1 END) AS female_count,
       COUNT(CASE WHEN gender = 'M' THEN 1 END) AS male_count
FROM employees
GROUP BY department;
```

---

# 3. Window Functions (Most Frequently Tested Category)

### 3.1 Second / Nth Highest Salary
```sql
SELECT salary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employees
) t
WHERE rnk = 2;   -- change N here
```

### 3.2 RANK vs DENSE_RANK vs ROW_NUMBER (Ties Handling)
```sql
SELECT name, salary,
       RANK()       OVER (ORDER BY salary DESC) AS rnk,        -- skips numbers after tie
       DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rnk,  -- no skipping
       ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num     -- always unique
FROM employees;
```

### 3.3 Top N per Group
```sql
SELECT department, name, salary
FROM (
    SELECT department, name, salary,
           ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
    FROM employees
) t
WHERE rn <= 3;
```

### 3.4 Running Total / Cumulative Sum
```sql
SELECT sale_date, amount,
       SUM(amount) OVER (ORDER BY sale_date
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM sales;
```

### 3.5 Moving / Rolling Average (7-day)
```sql
SELECT sale_date, amount,
       AVG(amount) OVER (ORDER BY sale_date
                          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg
FROM sales;
```

### 3.6 LAG / LEAD — Month-over-Month Growth
```sql
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
       ROUND((revenue - LAG(revenue) OVER (ORDER BY month))
             / LAG(revenue) OVER (ORDER BY month) * 100, 2) AS growth_pct
FROM monthly_revenue;
```

### 3.7 FIRST_VALUE / LAST_VALUE — First & Last Purchase per Customer
```sql
SELECT DISTINCT customer_id,
       FIRST_VALUE(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS first_order,
       LAST_VALUE(order_date)  OVER (PARTITION BY customer_id ORDER BY order_date
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order
FROM orders;
```

### 3.8 NTILE — Split Employees into Salary Quartiles
```sql
SELECT name, salary,
       NTILE(4) OVER (ORDER BY salary DESC) AS quartile
FROM employees;
```

### 3.9 Percentage of Total (Contribution %)
```sql
SELECT region, SUM(sales) AS region_sales,
       ROUND(SUM(sales) * 100.0 / SUM(SUM(sales)) OVER (), 2) AS pct_of_total
FROM sales
GROUP BY region;
```

### 3.10 Percent Rank / Cumulative Distribution
```sql
SELECT name, salary,
       PERCENT_RANK() OVER (ORDER BY salary) AS pct_rank,
       CUME_DIST()    OVER (ORDER BY salary) AS cum_dist
FROM employees;
```

### 3.11 Difference Between Current Row and Group Average
```sql
SELECT name, department, salary,
       salary - AVG(salary) OVER (PARTITION BY department) AS diff_from_dept_avg
FROM employees;
```

### 3.12 Count of Distinct Values in a Window
```sql
SELECT customer_id, order_date,
       COUNT(DISTINCT product_id) OVER (PARTITION BY customer_id) AS distinct_products
FROM orders;
```

---

# 4. Deduplication

### 4.1 Find Duplicate Rows
```sql
SELECT email, COUNT(*) AS cnt
FROM users
GROUP BY email
HAVING COUNT(*) > 1;
```

### 4.2 Delete Duplicates, Keep Lowest ID
```sql
DELETE FROM users
WHERE id NOT IN (
    SELECT MIN(id) FROM users GROUP BY email
);
```

### 4.3 Delete Duplicates Using Window Function (works when no unique key)
```sql
DELETE FROM users
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at) AS rn
        FROM users
    ) t
    WHERE rn > 1
);
```

### 4.4 Select Distinct Rows Based on Latest Record per Key
```sql
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
    FROM customer_snapshots
) t
WHERE rn = 1;
```

---

# 5. Gaps & Islands / Sessionization (Very Common at Amazon, Uber, LinkedIn)

### 5.1 Consecutive Login Streaks (3+ Days)
```sql
WITH ranked AS (
    SELECT user_id, login_date,
           DATE_SUB(login_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date)) AS grp
    FROM logins
)
SELECT user_id, MIN(login_date) AS streak_start, MAX(login_date) AS streak_end, COUNT(*) AS streak_length
FROM ranked
GROUP BY user_id, grp
HAVING COUNT(*) >= 3;
```

### 5.2 Group Consecutive Numeric IDs into Ranges (Islands)
```sql
WITH islands AS (
    SELECT id, id - ROW_NUMBER() OVER (ORDER BY id) AS grp
    FROM sequence_table
)
SELECT MIN(id) AS range_start, MAX(id) AS range_end
FROM islands
GROUP BY grp;
```

### 5.3 Find Missing IDs in a Sequence (Gaps)
```sql
SELECT t1.id + 1 AS missing_id
FROM numbers t1
LEFT JOIN numbers t2 ON t1.id + 1 = t2.id
WHERE t2.id IS NULL
ORDER BY missing_id;
```

### 5.3a Attendance Break — Find the Exact Day a User's Streak Broke
**Q:** For each user, find every date where they were present but were absent the day before (i.e., the day a new streak/attendance run starts after a break).
```sql
WITH attendance AS (
    SELECT user_id, attendance_date,
           LAG(attendance_date) OVER (PARTITION BY user_id ORDER BY attendance_date) AS prev_date
    FROM attendance_log
    WHERE status = 'Present'
)
SELECT user_id, attendance_date AS streak_restart_date
FROM attendance
WHERE prev_date IS NULL
   OR DATEDIFF(attendance_date, prev_date) > 1;
```

### 5.3b Longest Gap (Break) Between Logins per User
**Q:** Find the longest gap in days between two consecutive logins for each user.
```sql
WITH gaps AS (
    SELECT user_id, login_date,
           DATEDIFF(login_date, LAG(login_date) OVER (PARTITION BY user_id ORDER BY login_date)) AS gap_days
    FROM logins
)
SELECT user_id, MAX(gap_days) AS longest_break_days
FROM gaps
GROUP BY user_id;
```

### 5.3c First Day the Attendance Streak Broke After N Consecutive Days
**Q:** Find the first absence date for each user after they had at least N (e.g., 5) consecutive present days.
```sql
WITH ranked AS (
    SELECT user_id, attendance_date,
           DATE_SUB(attendance_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY attendance_date)) AS grp
    FROM attendance_log
    WHERE status = 'Present'
),
streaks AS (
    SELECT user_id, grp, MIN(attendance_date) AS streak_start, MAX(attendance_date) AS streak_end,
           COUNT(*) AS streak_length
    FROM ranked
    GROUP BY user_id, grp
)
SELECT user_id, streak_end AS last_present_day,
       DATE_ADD(streak_end, 1) AS break_day,
       streak_length
FROM streaks
WHERE streak_length >= 5
ORDER BY user_id, streak_end;
```

### 5.4 Sessionization — New Session if Gap > 30 Minutes
```sql
WITH gaps AS (
    SELECT user_id, event_time,
           LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time
    FROM events
),
flagged AS (
    SELECT *,
           CASE WHEN prev_time IS NULL
                     OR (UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(prev_time)) > 1800
                THEN 1 ELSE 0 END AS new_session
    FROM gaps
)
SELECT *, SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
FROM flagged;
```

### 5.5 Subscription Active Periods — Merge Overlapping Date Ranges
```sql
WITH ordered AS (
    SELECT customer_id, start_date, end_date,
           MAX(end_date) OVER (PARTITION BY customer_id
                                ORDER BY start_date
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max_end
    FROM subscriptions
),
flagged AS (
    SELECT *,
           CASE WHEN prev_max_end IS NULL OR start_date > prev_max_end THEN 1 ELSE 0 END AS new_group
    FROM ordered
),
grouped AS (
    SELECT *, SUM(new_group) OVER (PARTITION BY customer_id ORDER BY start_date) AS grp
    FROM flagged
)
SELECT customer_id, MIN(start_date) AS merged_start, MAX(end_date) AS merged_end
FROM grouped
GROUP BY customer_id, grp;
```

### 5.6 Repeat Purchase of Same Product Within 7 Days
```sql
WITH ordered AS (
    SELECT customer_id, product_id, order_date,
           LAG(order_date) OVER (PARTITION BY customer_id, product_id ORDER BY order_date) AS prev_date
    FROM orders
)
SELECT customer_id, product_id, order_date
FROM ordered
WHERE DATEDIFF(order_date, prev_date) <= 7;
```

---

# 6. Hierarchical / Recursive Queries

### 6.1 Org Chart — All Employees Under a Manager
```sql
WITH RECURSIVE org_chart AS (
    SELECT id, name, manager_id, 1 AS level
    FROM employees
    WHERE id = 1
    UNION ALL
    SELECT e.id, e.name, e.manager_id, oc.level + 1
    FROM employees e
    JOIN org_chart oc ON e.manager_id = oc.id
)
SELECT * FROM org_chart;
```

### 6.2 Find the Root Manager for Every Employee
```sql
WITH RECURSIVE chain AS (
    SELECT id, name, manager_id, id AS root_id
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, c.root_id
    FROM employees e
    JOIN chain c ON e.manager_id = c.id
)
SELECT * FROM chain;
```

### 6.3 Category Tree — Parent/Child Product Categories
```sql
WITH RECURSIVE cat_tree AS (
    SELECT category_id, category_name, parent_id, 0 AS depth
    FROM categories
    WHERE parent_id IS NULL
    UNION ALL
    SELECT c.category_id, c.category_name, c.parent_id, ct.depth + 1
    FROM categories c
    JOIN cat_tree ct ON c.parent_id = ct.category_id
)
SELECT * FROM cat_tree ORDER BY depth;
```

---

# 7. Retention, Cohort & Funnel Analysis

### 7.1 Day-1 Retention by Signup Cohort
```sql
WITH first_activity AS (
    SELECT user_id, MIN(activity_date) AS cohort_date
    FROM activity
    GROUP BY user_id
)
SELECT f.cohort_date, COUNT(DISTINCT a.user_id) AS retained_users
FROM first_activity f
JOIN activity a
  ON f.user_id = a.user_id
 AND a.activity_date = DATE_ADD(f.cohort_date, 1)
GROUP BY f.cohort_date;
```

### 7.2 Funnel Conversion Rate (View → Cart → Purchase)
```sql
SELECT
    COUNT(DISTINCT CASE WHEN event = 'view'     THEN user_id END) AS viewed,
    COUNT(DISTINCT CASE WHEN event = 'cart'     THEN user_id END) AS added_cart,
    COUNT(DISTINCT CASE WHEN event = 'purchase' THEN user_id END) AS purchased,
    ROUND(COUNT(DISTINCT CASE WHEN event = 'purchase' THEN user_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN event = 'view' THEN user_id END), 0), 2) AS conversion_rate
FROM events;
```

### 7.3 Monthly Active Users (MAU) & Retention from Prior Month
```sql
WITH monthly_users AS (
    SELECT DISTINCT user_id, DATE_TRUNC('month', activity_date) AS month
    FROM activity
)
SELECT curr.month,
       COUNT(DISTINCT curr.user_id) AS active_users,
       COUNT(DISTINCT prev.user_id) AS retained_from_prev_month
FROM monthly_users curr
LEFT JOIN monthly_users prev
  ON curr.user_id = prev.user_id
 AND prev.month = ADD_MONTHS(curr.month, -1)
GROUP BY curr.month;
```

### 7.4 New vs Returning Customers per Month
```sql
WITH first_purchase AS (
    SELECT customer_id, MIN(DATE_TRUNC('month', order_date)) AS first_month
    FROM orders
    GROUP BY customer_id
)
SELECT DATE_TRUNC('month', o.order_date) AS month,
       COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', o.order_date) = f.first_month
                            THEN o.customer_id END) AS new_customers,
       COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', o.order_date) > f.first_month
                            THEN o.customer_id END) AS returning_customers
FROM orders o
JOIN first_purchase f ON o.customer_id = f.customer_id
GROUP BY DATE_TRUNC('month', o.order_date);
```

---

# 8. Pivot / Unpivot / Reshape Data

### 8.1 Pivot Rows to Columns (Quarterly Sales)
```sql
SELECT product,
       SUM(CASE WHEN quarter = 'Q1' THEN sales ELSE 0 END) AS Q1,
       SUM(CASE WHEN quarter = 'Q2' THEN sales ELSE 0 END) AS Q2,
       SUM(CASE WHEN quarter = 'Q3' THEN sales ELSE 0 END) AS Q3,
       SUM(CASE WHEN quarter = 'Q4' THEN sales ELSE 0 END) AS Q4
FROM sales_data
GROUP BY product;
```

### 8.2 Using PIVOT Clause (Databricks / Spark SQL)
```sql
SELECT *
FROM sales_data
PIVOT (
    SUM(sales) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4')
);
```

### 8.3 Unpivot Columns Back to Rows
```sql
SELECT product, quarter, sales
FROM sales_wide
UNPIVOT (
    sales FOR quarter IN (Q1, Q2, Q3, Q4)
);
```

---

# 9. Strings, Dates & JSON

### 9.1 Word Frequency Count
```sql
SELECT word, COUNT(*) AS frequency
FROM (
    SELECT EXPLODE(SPLIT(sentence, ' ')) AS word
    FROM sentences
) t
GROUP BY word
ORDER BY frequency DESC;
```

### 9.2 Extract Domain from Email
```sql
SELECT email, SUBSTRING(email, INSTR(email, '@') + 1) AS domain
FROM users;
```

### 9.3 Find Palindromes
```sql
SELECT word
FROM words
WHERE word = REVERSE(word);
```

### 9.4 Date Difference in Days / Months
```sql
SELECT DATEDIFF(end_date, start_date)       AS days_diff,
       MONTHS_BETWEEN(end_date, start_date) AS months_diff
FROM projects;
```

### 9.5 Extract Fields from a JSON Column
```sql
SELECT raw_json,
       GET_JSON_OBJECT(raw_json, '$.user.id')   AS user_id,
       GET_JSON_OBJECT(raw_json, '$.user.name') AS user_name
FROM events_raw;
```

### 9.6 Flatten a Nested Array Column
```sql
SELECT order_id, item.product_id, item.qty
FROM orders
LATERAL VIEW EXPLODE(items) exploded_table AS item;
```

---

# 10. Set Operations & Subqueries

### 10.1 UNION vs UNION ALL
```sql
-- Removes duplicates (slower, extra sort/distinct step)
SELECT customer_id FROM online_orders
UNION
SELECT customer_id FROM store_orders;

-- Keeps duplicates (faster, no dedup step)
SELECT customer_id FROM online_orders
UNION ALL
SELECT customer_id FROM store_orders;
```

### 10.2 INTERSECT — Customers Who Bought in Both Online & Store
```sql
SELECT customer_id FROM online_orders
INTERSECT
SELECT customer_id FROM store_orders;
```

### 10.3 EXCEPT / MINUS — Customers Who Only Bought Online
```sql
SELECT customer_id FROM online_orders
EXCEPT
SELECT customer_id FROM store_orders;
```

### 10.4 Correlated Subquery — Employees Earning Above Dept Average
```sql
SELECT e.name, e.salary, e.department
FROM employees e
WHERE e.salary > (
    SELECT AVG(e2.salary)
    FROM employees e2
    WHERE e2.department = e.department
);
```

### 10.5 EXISTS vs IN (Performance Concept)
```sql
-- EXISTS is generally more efficient on large correlated subqueries (short-circuits, NULL-safe)
SELECT c.customer_id
FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
```

---

# 11. Advanced / Company-Favorite Scenario Questions

### 11.1 Median Salary
```sql
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees;
```

### 11.2 Swap Values (Gender Flag) Without a Temp Column
```sql
UPDATE users
SET gender = CASE WHEN gender = 'M' THEN 'F' ELSE 'M' END;
```

### 11.3 Employees with No Duplicate Skill Entries
```sql
SELECT employee_id
FROM employee_skills
GROUP BY employee_id
HAVING COUNT(DISTINCT skill) = COUNT(skill);
```

### 11.4 Product with Second Highest Sales in Each Category
```sql
SELECT category, product, total_sales
FROM (
    SELECT category, product, SUM(sales) AS total_sales,
           DENSE_RANK() OVER (PARTITION BY category ORDER BY SUM(sales) DESC) AS rnk
    FROM sales
    GROUP BY category, product
) t
WHERE rnk = 2;
```

### 11.5 Manager Paid Less Than a Direct Report (Salary Anomaly)
```sql
SELECT m.name AS manager, m.salary AS manager_salary
FROM employees m
WHERE m.salary < (
    SELECT MIN(e.salary) FROM employees e WHERE e.manager_id = m.id
);
```

### 11.6 Longest Consecutive Streak of a Value (e.g., "Win" Streak)
```sql
WITH flagged AS (
    SELECT player_id, match_date, result,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY match_date)
           - ROW_NUMBER() OVER (PARTITION BY player_id, result ORDER BY match_date) AS grp
    FROM matches
)
SELECT player_id, result, COUNT(*) AS streak_length, MIN(match_date) AS streak_start, MAX(match_date) AS streak_end
FROM flagged
WHERE result = 'Win'
GROUP BY player_id, result, grp
ORDER BY streak_length DESC;
```

### 11.7 Year-over-Year Comparison (Same Month, Different Year)
```sql
SELECT curr.month, curr.revenue AS curr_year_revenue, prev.revenue AS prev_year_revenue,
       ROUND((curr.revenue - prev.revenue) / prev.revenue * 100, 2) AS yoy_growth_pct
FROM monthly_revenue curr
JOIN monthly_revenue prev
  ON curr.month = prev.month
 AND curr.year = prev.year + 1;
```

### 11.8 Employees Who Joined and Left Within 90 Days (Attrition Flag)
```sql
SELECT employee_id, join_date, exit_date
FROM employees
WHERE exit_date IS NOT NULL
  AND DATEDIFF(exit_date, join_date) <= 90;
```

### 11.9 Rank Products by Sales, Ties Get Same Rank, No Gaps
```sql
SELECT product, sales,
       DENSE_RANK() OVER (ORDER BY sales DESC) AS rank
FROM product_sales;
```

### 11.10 Find Overlapping Meeting/Room Booking Times
```sql
SELECT a.meeting_id AS meeting1, b.meeting_id AS meeting2
FROM meetings a
JOIN meetings b
  ON a.room_id = b.room_id
 AND a.meeting_id < b.meeting_id
 AND a.start_time < b.end_time
 AND b.start_time < a.end_time;
```

---

# 12. SQL Theory / Conceptual Questions (Frequent Follow-ups)

| Question | Short Answer |
|---|---|
| Clustered vs Non-Clustered Index | Clustered index physically sorts/stores table data in index order (one per table); non-clustered has a separate structure with pointers to actual rows (multiple allowed) |
| Primary Key vs Unique Key | Primary key disallows NULLs and is one per table; Unique key allows one NULL (in most RDBMS) and multiple per table |
| Normalization vs Denormalization | Normalization reduces redundancy by splitting into related tables; denormalization merges data back for read performance |
| ACID Properties | Atomicity, Consistency, Isolation, Durability — guarantees reliable transaction processing |
| View vs Materialized View | A view is a virtual, always-live query; a materialized view stores precomputed results physically and needs refreshing |
| WHERE vs HAVING | WHERE filters rows before aggregation; HAVING filters groups after aggregation |
| DELETE vs TRUNCATE vs DROP | DELETE removes rows (logged, rollback-able, can use WHERE); TRUNCATE removes all rows fast (minimal logging); DROP removes the entire table structure |
| Index Trade-off | Indexes speed up reads (SELECT/WHERE/JOIN) but slow down writes (INSERT/UPDATE/DELETE) due to index maintenance |
| Star Schema vs Snowflake Schema | Star schema has denormalized flat dimension tables; snowflake schema normalizes dimensions into sub-tables |
| Slowly Changing Dimension (SCD Type 2) | Tracks historical changes by adding new rows with effective/expiry dates instead of overwriting |
| CTE vs Subquery vs Temp Table | CTE improves readability and can be recursive; subquery is inline and scoped to one query; temp table persists for the session and can be indexed |
| Window Function vs GROUP BY | GROUP BY collapses rows into one per group; window functions keep all rows while computing aggregate/analytic values per partition |

---

# 13. Databricks SQL / Delta Lake — Key Function Cheat Sheet (with Examples)

| Function / Command | Purpose | Example |
|---|---|---|
| `EXPLODE()` | Splits an array/map column into multiple rows | `SELECT id, EXPLODE(items) AS item FROM orders;` → one row per item in the array |
| `POSEXPLODE()` | Like EXPLODE but also returns the element's position/index | `SELECT id, POSEXPLODE(items) AS (pos, item) FROM orders;` → adds a 0-based index column |
| `COLLECT_LIST()` | Aggregates column values into an array (keeps duplicates) | `SELECT customer_id, COLLECT_LIST(product) FROM orders GROUP BY customer_id;` → array of all products bought, dupes included |
| `COLLECT_SET()` | Aggregates column values into an array (unique values only) | `SELECT customer_id, COLLECT_SET(product) FROM orders GROUP BY customer_id;` → array of distinct products bought |
| `LATERAL VIEW` | Used with EXPLODE to flatten nested arrays/structs into rows | `SELECT id, item.product_id FROM orders LATERAL VIEW EXPLODE(items) t AS item;` |
| `TRANSFORM()` | Applies a lambda function to each element of an array | `SELECT TRANSFORM(prices, x -> x * 1.1) FROM products;` → increases every price in the array by 10% |
| `FILTER()` | Filters array elements based on a lambda condition | `SELECT FILTER(scores, x -> x > 50) FROM students;` → keeps only scores above 50 |
| `AGGREGATE()` | Reduces an array to a single value using an accumulator (like fold/reduce) | `SELECT AGGREGATE(prices, 0, (acc, x) -> acc + x) FROM cart;` → sums all prices in the array |
| `ARRAY_DISTINCT()` | Removes duplicate elements from an array | `SELECT ARRAY_DISTINCT(ARRAY(1,2,2,3));` → `[1,2,3]` |
| `ARRAYS_ZIP()` | Merges multiple arrays element-wise into structs | `SELECT ARRAYS_ZIP(ARRAY('a','b'), ARRAY(1,2));` → `[{a,1},{b,2}]` |
| `ARRAY_SORT()` | Sorts elements of an array | `SELECT ARRAY_SORT(ARRAY(3,1,2));` → `[1,2,3]` |
| `GET_JSON_OBJECT()` | Extracts a value from a JSON string using a path expression | `SELECT GET_JSON_OBJECT(raw_json, '$.user.id') FROM events;` → pulls `user.id` out of a JSON string |
| `FROM_JSON()` | Parses a JSON string into a struct/map based on a given schema | `SELECT FROM_JSON(raw_json, 'user STRUCT<id:INT, name:STRING>') FROM events;` |
| `TO_JSON()` | Converts a struct/map column into a JSON string | `SELECT TO_JSON(STRUCT(id, name)) FROM users;` → `{"id":1,"name":"Sam"}` |
| `PARSE_JSON()` | Parses text into a semi-structured VARIANT type | `SELECT PARSE_JSON('{"a":1}');` → queryable VARIANT value |
| `SCHEMA_OF_JSON()` | Infers the schema of a JSON string | `SELECT SCHEMA_OF_JSON('{"a":1,"b":"x"}');` → returns `STRUCT<a:BIGINT,b:STRING>` |
| `DATE_TRUNC()` | Truncates a date/timestamp to a specified unit (month, year, etc.) | `SELECT DATE_TRUNC('MONTH', order_date) FROM orders;` → rounds every date down to the 1st of its month |
| `DATEDIFF()` | Returns the number of days between two dates | `SELECT DATEDIFF('2026-08-30','2026-08-01');` → `29` |
| `DATE_ADD()` / `DATE_SUB()` | Adds/subtracts a number of days from a date | `SELECT DATE_ADD(order_date, 7) FROM orders;` → order date + 7 days |
| `ADD_MONTHS()` | Adds a given number of months to a date | `SELECT ADD_MONTHS('2026-01-15', 3);` → `2026-04-15` |
| `MONTHS_BETWEEN()` | Returns the number of months between two dates | `SELECT MONTHS_BETWEEN('2026-06-01','2026-01-01');` → `5.0` |
| `TO_DATE()` / `TO_TIMESTAMP()` | Converts a string to a date/timestamp type | `SELECT TO_DATE('2026-08-30','yyyy-MM-dd');` |
| `UNIX_TIMESTAMP()` | Converts a timestamp to Unix epoch seconds | `SELECT UNIX_TIMESTAMP(event_time) FROM events;` |
| `WINDOW()` | Buckets timestamps into fixed time windows for streaming/batch aggregation | `SELECT WINDOW(event_time,'1 hour'), COUNT(*) FROM events GROUP BY WINDOW(event_time,'1 hour');` → hourly event counts |
| `RANK()` | Assigns rank with gaps left after ties | `RANK() OVER (ORDER BY salary DESC)` → ties get same rank, next rank skips (1,1,3) |
| `DENSE_RANK()` | Assigns rank without gaps after ties | `DENSE_RANK() OVER (ORDER BY salary DESC)` → ties get same rank, no skipping (1,1,2) |
| `ROW_NUMBER()` | Assigns a unique sequential number within a partition | `ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)` → 1,2,3... per department |
| `NTILE(n)` | Divides rows into n roughly equal-sized buckets | `NTILE(4) OVER (ORDER BY salary DESC)` → splits employees into 4 salary quartiles |
| `LAG()` / `LEAD()` | Accesses the previous/next row's value within a window | `LAG(revenue) OVER (ORDER BY month)` → previous month's revenue on the same row |
| `FIRST_VALUE()` / `LAST_VALUE()` | Returns the first/last value in a window frame | `FIRST_VALUE(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)` → each customer's first order date |
| `PERCENTILE_CONT()` / `PERCENTILE_APPROX()` | Computes exact / approximate percentile values | `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)` → median salary |
| `PIVOT()` | Rotates unique row values into multiple columns | `SELECT * FROM sales PIVOT (SUM(sales) FOR quarter IN ('Q1','Q2','Q3','Q4'));` → one column per quarter |
| `UNPIVOT()` | Converts columns back into rows | `SELECT * FROM sales_wide UNPIVOT (sales FOR quarter IN (Q1,Q2,Q3,Q4));` → columns become rows again |
| `MERGE INTO` | Upserts (insert/update/delete) rows in a Delta table based on a match condition | `MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;` |
| `MAP_FROM_ARRAYS()` | Creates a map from two arrays (keys array, values array) | `SELECT MAP_FROM_ARRAYS(ARRAY('a','b'), ARRAY(1,2));` → `{"a":1,"b":2}` |
| `SIZE()` | Returns the length of an array or map | `SELECT SIZE(items) FROM orders;` → number of items in the order |
| `SPLIT()` | Splits a string into an array using a delimiter | `SELECT SPLIT('a,b,c', ',');` → `['a','b','c']` |
| `CONCAT_WS()` | Concatenates strings with a specified separator, ignoring NULLs | `SELECT CONCAT_WS('-', city, state, zip) FROM addresses;` → `"Pune-MH-411001"` |
| `REGEXP_EXTRACT()` | Extracts a substring matching a regex pattern | `SELECT REGEXP_EXTRACT(email, '@(.+)', 1) FROM users;` → domain from email |
| `REGEXP_REPLACE()` | Replaces substrings matching a regex pattern | `SELECT REGEXP_REPLACE(phone, '[^0-9]', '') FROM users;` → strips non-digits from phone numbers |
| `COALESCE()` | Returns the first non-NULL value from a list of expressions | `SELECT COALESCE(phone, alt_phone, 'N/A') FROM users;` → falls back through options |
| `NULLIF()` | Returns NULL if two expressions are equal, else the first expression | `SELECT NULLIF(discount, 0) FROM orders;` → converts 0 discount to NULL |
| `TRY_CAST()` | Casts a value, returning NULL instead of erroring on failure | `SELECT TRY_CAST(amount_str AS DECIMAL(10,2)) FROM raw_data;` → bad values become NULL instead of failing the query |
| `IDENTITY` / `GENERATED ALWAYS AS IDENTITY` | Auto-increments a surrogate key column in a Delta table | `CREATE TABLE t (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING);` |
| `VACUUM` | Removes old/unreferenced data files from a Delta table to reclaim storage | `VACUUM sales_delta RETAIN 168 HOURS;` → cleans up files older than 7 days |
| `OPTIMIZE ... ZORDER BY` | Compacts small files and co-locates related data for faster reads | `OPTIMIZE sales_delta ZORDER BY (customer_id);` → speeds up queries filtering on customer_id |
| `DESCRIBE HISTORY` | Shows the version history/audit log of a Delta table | `DESCRIBE HISTORY sales_delta;` → lists every write/version with timestamp and operation |
| `VERSION AS OF` / `TIMESTAMP AS OF` | Time-travel query to read a previous version of a Delta table | `SELECT * FROM sales_delta VERSION AS OF 5;` → reads the table as it looked at version 5 |
| `RESTORE TABLE` | Reverts a Delta table to a previous version or timestamp | `RESTORE TABLE sales_delta TO VERSION AS OF 5;` |
| `DEEP CLONE` / `SHALLOW CLONE` | Creates a full copy / metadata-only copy of a Delta table | `CREATE TABLE sales_backup DEEP CLONE sales_delta;` → full independent copy of data + metadata |
| `CREATE OR REFRESH STREAMING TABLE` | Defines a Delta Live Tables streaming table that ingests incrementally | `CREATE OR REFRESH STREAMING TABLE raw_events AS SELECT * FROM STREAM(source_table);` |
| `APPLY_CHANGES INTO` (DLT) | Handles CDC upserts/deletes into a target streaming table | `APPLY CHANGES INTO target FROM STREAM(cdc_feed) KEYS (id) SEQUENCE BY updated_at;` |

---

## Quick Revision Checklist Before Interview
- [ ] Can write Nth highest/lowest value query from memory
- [ ] Comfortable with all window function types (RANK, ROW_NUMBER, LAG/LEAD, running total)
- [ ] Can solve gaps & islands / sessionization / attendance-break patterns from scratch
- [ ] Know self-joins and recursive CTEs
- [ ] Can explain EXISTS vs IN, WHERE vs HAVING, clustered vs non-clustered index
- [ ] Comfortable with pivot/unpivot and JSON/array functions
- [ ] Know Delta Lake specific commands (MERGE, OPTIMIZE, VACUUM, time travel)
