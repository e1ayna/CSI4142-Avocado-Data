import mysql.connector
import csv

# Connect to MySQL Database
conn = mysql.connector.connect(
    host="localhost",
    port="3307",
    user="root",
    password="Cl7397521623",
)

# Create cursor
cursor = conn.cursor()

cursor.execute("USE avocado") 

def print_results(results):
    for row in results:
        print(row)

# Roll down query: Drill down from year level to quarter level
roll_down_query = """
SELECT
    YEAR(d.date) AS Year,
    QUARTER(d.date) AS Quarter,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Date d ON s.date_id = d.date_id
GROUP BY
    YEAR(d.date), QUARTER(d.date);
"""

# Execute the roll down query
cursor.execute(roll_down_query)
roll_down_results = cursor.fetchall()


# Drill up query: Roll up from quarter level to year level
drill_up_query = """
SELECT
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Date d ON s.date_id = d.date_id
GROUP BY
    YEAR(d.date);
"""

# Execute the drill up query
cursor.execute(drill_up_query)
drill_up_results = cursor.fetchall()
# for row in drill_up_results:
#     print(row)

# Dice Query 1: Create a sub-cube for the year 2017 and Boston
dice_query_1 = """
SELECT
    r.region AS Region,
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Region r ON s.region_id = r.region_id
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    YEAR(d.date) = 2017
    AND r.region = 'Boston'
GROUP BY
    r.region, YEAR(d.date);
"""

# Execute the dice query 1
cursor.execute(dice_query_1)
dice_results_1 = cursor.fetchall()
# for row in dice_results_1:
#     print(row)

# Dice Query 2: Create a sub-cube for the month of January and California
dice_query_2 = """
SELECT
    r.region AS Region,
    MONTH(d.date) AS Month,
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Region r ON s.region_id = r.region_id
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    MONTH(d.date) = 1
    AND r.region = 'California'
GROUP BY
    r.region, MONTH(d.date), YEAR(d.date);
"""

# Execute the dice query 2
cursor.execute(dice_query_2)
dice_results_2 = cursor.fetchall()
# for row in dice_results_2:
#     print(row)

# d. Combining OLAP operations

# Query 1: OLAP operation combining slice and roll up - Analyzing total sales volume per quarter for the year 2018
# Slicing the data by year and rolling up to quarter level
quarterly_sales_query = """
SELECT
    YEAR(d.date) AS Year,
    QUARTER(d.date) AS Quarter,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    YEAR(d.date) = 2018
GROUP BY
    YEAR(d.date), QUARTER(d.date);
"""

# Execute the query
cursor.execute(quarterly_sales_query)
quarterly_sales_records = cursor.fetchall()
# for record in quarterly_sales_records:
#     print(record)

# Query 2: OLAP operation combining slice and drill down - Analyzing total sales volume per region for the year 2018
# Slicing the data by year and drilling down to region level
region_sales_query = """
SELECT
    r.region AS Region,
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Date d ON s.date_id = d.date_id
JOIN
    Region r ON s.region_id = r.region_id
WHERE
    YEAR(d.date) = 2018
GROUP BY
    r.region, YEAR(d.date);
"""

# Execute the query
cursor.execute(region_sales_query)
region_sales_records = cursor.fetchall()
# for record in region_sales_records:
#     print(record)


# Query 3: OLAP operation combining slice and dice - Analyzing total sales volume for a specific region in a particular month
# Slicing the data by year and drilling down to month level, then dicing for a specific region
specific_region_sales_query = """
SELECT
    r.region AS Region,
    MONTH(d.date) AS Month,
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Region r ON s.region_id = r.region_id
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    YEAR(d.date) = 2018
    AND r.region = 'Boston'  -- Specific region
GROUP BY
    r.region, MONTH(d.date), YEAR(d.date);
"""

# Execute the query
cursor.execute(specific_region_sales_query)
specific_region_sales_records = cursor.fetchall()
# for record in specific_region_sales_records:
#     print(record)

# Query 4: OLAP operation combining slice and drill up - Analyzing total sales volume per year
# Slicing the data by year
yearly_sales_query = """
SELECT
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    YEAR(d.date) >= 2016 AND YEAR(d.date) <= 2018
GROUP BY
    YEAR(d.date);
"""

# Execute the query
cursor.execute(yearly_sales_query)
yearly_sales_records = cursor.fetchall()
# for record in yearly_sales_records:
#     print(record)



# Iceberg query to retrieve the total volume of sales for each region in the year 2016, 
# providing a sliced view of the data focused specifically on that year
slice_query = """
SELECT
    r.region AS Region,
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Region r ON s.region_id = r.region_id
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    YEAR(d.date) = 2016
GROUP BY
    r.region, YEAR(d.date);
"""

# Execute the slice query
cursor.execute(slice_query)
olap_results = cursor.fetchall()
# for row in olap_results:
#     print(row)

# Iceberg query to determine the top 10 regions with the highest volume for the year 2017
top_regions_query = """
SELECT
    r.region,
    SUM(s.total_volume) AS total_volume
FROM
    Sales s
JOIN
    Region r ON s.region_id = r.region_id
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    YEAR(d.date) = 2017
GROUP BY
    r.region
ORDER BY
    total_volume DESC
LIMIT 10;
"""

# Execute the query to get the top 10 regions with the highest volume for 2017
cursor.execute(top_regions_query)
top_regions_records = cursor.fetchall()
# print("\nTop 10 Regions with Highest Volume in 2017:")
# for record in top_regions_records:
#     print(record)


# Window query to calculate the total volume of sales for each region over time
windowing_query = """
SELECT
    r.region,
    d.date,
    SUM(s.total_volume) OVER(PARTITION BY r.region ORDER BY d.date) AS cumulative_volume
FROM
    Sales s
JOIN
    Region r ON s.region_id = r.region_id
JOIN
    Date d ON s.date_id = d.date_id
ORDER BY
    r.region, d.date;
"""

# Execute the windowing query 
cursor.execute(windowing_query)
windowing_records = cursor.fetchall()
# print("\nWindowing Query Results:")
# for record in windowing_records:
#     print(record)


# Query using the WINDOW clause to compare the average price in each region in 2017 to that of the previous and next years
window_query = """
WITH AvgPriceComparison AS (
    SELECT
        r.region AS Region,
        YEAR(d.date) AS Year,
        AVG(s.average_price) AS AvgPrice,
        LAG(AVG(s.average_price)) OVER wnd AS AvgPricePrevYear,
        LEAD(AVG(s.average_price)) OVER wnd AS AvgPriceNextYear
    FROM
        Sales s
    JOIN
        Region r ON s.region_id = r.region_id
    JOIN
        Date d ON s.date_id = d.date_id
    WHERE
        YEAR(d.date) BETWEEN 2016 AND 2018
    GROUP BY
        r.region, YEAR(d.date)
    WINDOW
        wnd AS (PARTITION BY r.region ORDER BY YEAR(d.date))
)
SELECT *
FROM AvgPriceComparison
WHERE Year = 2017;
"""

# Execute the query with the Window clause
cursor.execute(window_query)
window_records = cursor.fetchall()
# print("\nWindow Query Results:")
# for record in window_records:
# print(record)
# Save results to a file
with open("OLAP_Results.txt", "w") as file:
    file.write(" ==== Roll Down Results ==== \n")
    for row in roll_down_results:
        file.write(str(row) + "\n")
    file.write(" ==== Drill Up Results ==== \n")
    for row in drill_up_results:
        file.write(str(row) + "\n")
    file.write(" ==== Dice Results 1 ==== \n")
    for row in dice_results_1:
        file.write(str(row) + "\n")
    file.write(" ==== Dice Results 2 ==== \n")
    for row in dice_results_2:
        file.write(str(row) + "\n")
    file.write(" ==== Quarterly Sales Records ==== \n")
    for record in quarterly_sales_records:
        file.write(str(record) + "\n")
    file.write(" ==== Region Sales Records ==== \n")
    for record in region_sales_records:
        file.write(str(record) + "\n")
    file.write(" ==== Specific Region Sales Records ==== \n")
    for record in specific_region_sales_records:
        file.write(str(record) + "\n")
    file.write(" ==== Yearly Sales Records ==== \n")
    for record in yearly_sales_records:
        file.write(str(record) + "\n")
    file.write(" ==== OLAP Results ==== \n")
    for row in olap_results:
        file.write(str(row) + "\n")
    file.write(" ==== Top Regions Records ==== \n")
    for record in top_regions_records:
        file.write(str(record) + "\n")
    file.write(" ==== Windowing Records ==== \n")
    for record in windowing_records:
        file.write(str(record) + "\n")
    file.write(" ==== Window Records ==== \n")
    for record in window_records:
        file.write(str(record) + "\n")
# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()
