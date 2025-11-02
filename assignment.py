import duckdb

# Step 1: Connect / create db
con = duckdb.connect('sql_advanced_1_5.db')
print("✅ Connected to DuckDB successfully.")

# Step 2: Setup base tables
con.execute("""
CREATE OR REPLACE TABLE departments (
    dept_id INTEGER PRIMARY KEY,
    dept_name VARCHAR NOT NULL
);
""")

con.execute("""
CREATE OR REPLACE TABLE employees (
    emp_id INTEGER PRIMARY KEY,
    emp_name VARCHAR,
    dept_id INTEGER,
    salary DECIMAL(10,2),
    hire_date DATE,
    FOREIGN KEY(dept_id) REFERENCES departments(dept_id)
);
""")

con.execute("""
INSERT INTO departments VALUES
(1, 'HR'), (2, 'Finance'), (3, 'IT');
""")

con.execute("""
INSERT INTO employees VALUES
(1, 'Alice', 1, 5200.00, '2022-04-01'),
(2, 'Bob', 2, 6700.00, '2023-02-10'),
(3, 'Charlie', 3, 7200.00, '2021-12-15'),
(4, 'Dina', 2, 6100.00, '2022-06-30'),
(5, 'Evan', 3, 8200.00, '2020-08-19'),
(6, 'Fiona', 1, 5600.00, '2021-01-10');
""")
print("✅ Sample data inserted.\n")

# Step 3: Join example
print("🤝 INNER JOIN employees + departments:")
rows = con.execute("""
SELECT e.emp_name, d.dept_name, e.salary
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
ORDER BY d.dept_name;
""").fetchall()
print(rows, "\n")

# Step 4: Union example
print("➕ UNION example (high vs low earners):")
rows = con.execute("""
SELECT emp_name, 'High Earner' AS category FROM employees WHERE salary >= 7000
UNION
SELECT emp_name, 'Low Earner' AS category FROM employees WHERE salary < 7000;
""").fetchall()
print(rows, "\n")

# Step 5: Window function example
print("🏆 RANK employees by salary within each department:")
rows = con.execute("""
SELECT dept_id, emp_name, salary,
RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rank_in_dept
FROM employees;
""").fetchall()
print(rows, "\n")

# Step 6: Subquery example
print("🔍 Employees earning above department average:")
rows = con.execute("""
SELECT emp_name, salary, dept_id
FROM employees e
WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept_id = e.dept_id);
""").fetchall()
print(rows, "\n")

# Step 7: Common Table Expression (CTE)
print("🧱 Average salary per dept using CTE:")
rows = con.execute("""
WITH dept_avg AS (
    SELECT dept_id, AVG(salary) AS avg_sal FROM employees GROUP BY dept_id
)
SELECT d.dept_name, ROUND(a.avg_sal,2) AS avg_salary
FROM dept_avg a
JOIN departments d ON a.dept_id = d.dept_id;
""").fetchall()
print(rows, "\n")

# Step 8: Meta query
print("📋 Tables in database:")
print(con.execute("PRAGMA show_tables;").fetchall())

con.close()
print("\n✅ Connection closed.")
