import mysql.connector
import csv

# Connect to MySQL Database
conn = mysql.connector.connect(
    host="localhost",
    port="3307",
    user="root",
    password="password",
)

# Create cursor
cursor = conn.cursor()

# Drop the existing database if it exists
# cursor.execute("DROP DATABASE IF EXISTS avocado")

# Recreate the database
# cursor.execute("CREATE DATABASE avocado")

# Use the database
# cursor.execute("USE avocado")

# Create database
cursor.execute("CREATE DATABASE IF NOT EXISTS avocado")
cursor.execute("USE avocado")

# Create Dimension Tables
create_date_table_query = """
CREATE TABLE IF NOT EXISTS Date (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    CONSTRAINT uc_date UNIQUE (date)
);
"""

create_product_table_query = """
CREATE TABLE IF NOT EXISTS Product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    plu_code_4046 INT NOT NULL,
    plu_code_4225 INT NOT NULL,
    plu_code_4770 INT NOT NULL,
    CONSTRAINT uc_plu_codes UNIQUE (plu_code_4046, plu_code_4225, plu_code_4770)
);
"""

create_type_table_query = """
CREATE TABLE IF NOT EXISTS Type (
    type_id INT AUTO_INCREMENT PRIMARY KEY,
    type VARCHAR(20) NOT NULL,
    CONSTRAINT uc_type_name UNIQUE (type)
);
"""

create_region_table_query = """
CREATE TABLE IF NOT EXISTS Region (
    region_id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(50) NOT NULL,
    CONSTRAINT uc_region_name UNIQUE (region)
);
"""

create_bags_table_query = """
CREATE TABLE IF NOT EXISTS Bags (
    bags_id INT AUTO_INCREMENT PRIMARY KEY,
    small_bags INT NOT NULL,
    large_bags INT NOT NULL,
    xlarge_bags INT NOT NULL,
    CONSTRAINT uc_plu_codes UNIQUE (small_bags, large_bags, xlarge_bags)
);
"""

# Execute the queries to create dimension tables
cursor.execute(create_date_table_query)
cursor.execute(create_product_table_query)
cursor.execute(create_type_table_query)
cursor.execute(create_region_table_query)
cursor.execute(create_bags_table_query)

# Function to insert data into the specified table
def insert_data(table_name, columns, data):
    # Prepare the SQL query for insertion
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in columns])})"
     # Check for existing records
    existing_query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE {' AND '.join([f'{col} = %s' for col in columns])}"
    # Execute the query to insert data without duplicates
    for record in data:
        cursor.execute(existing_query, record)
        result = cursor.fetchone()
        if result is None:
            cursor.execute(insert_query, record)

# Read data from CSV file and insert into Date table
with open('stagged_data.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    date_data = [(row['Date'],) for row in csv_reader]  # Assuming 'Date' column is for date
    insert_data('Date', ['date'], date_data)

# Read data from CSV file and insert into Product table
with open('stagged_data.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    next(csv_reader)  # Skip header row

    product_data = [(int(row['4046']), int(row['4225']), int(row['4770'])) for row in csv_reader]
    insert_product_query = """INSERT IGNORE INTO Product (plu_code_4046, plu_code_4225, plu_code_4770) VALUES (%s, %s, %s)
    """
    cursor.executemany(insert_product_query, product_data)

    # Populate Type table
    type_data = [(row['type_encoded'],) for row in csv_reader]  # Sample data for type dimension
    insert_data('Type', ['type'], type_data)

# Read data from CSV file again to reset the file cursor
with open('stagged_data.csv', 'r') as file:
    csv_reader = csv.DictReader(file)

    region_data = [(row['region'],) for row in csv_reader]  # Assuming 'region' column contains region names
    insert_data('Region', ['region'], region_data)

with open('stagged_data.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    next(csv_reader)  # Skip header row

    bags_data = [(int(row['Small Bags']), int(row['Large Bags']), int(row['XLarge Bags'])) for row in csv_reader]
    insert_bags_query = """INSERT IGNORE INTO Bags (small_bags, large_bags, xlarge_bags) VALUES (%s, %s, %s)
    """
    cursor.executemany(insert_bags_query, bags_data)

# Read data from CSV file and insert into Type table
with open('stagged_data.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    next(csv_reader)  # Skip header row
    type_data = [(row['type_encoded'],) for row in csv_reader]  
    insert_data('Type', ['type'], type_data)


# View the tables
# cursor.execute("SELECT * FROM Date")
# date_records = cursor.fetchall()
# print("Date Table:")
# for record in date_records:
#     print(record)

# cursor.execute("SELECT * FROM Product")
# product_records = cursor.fetchall()
# print("\nProduct Table:")
# for record in product_records:
#     print(record)

# cursor.execute("SELECT * FROM Type")
# type_records = cursor.fetchall()
# print("\nType Table:")
# for record in type_records:
#     print(record)

# cursor.execute("SELECT * FROM Region")
# region_records = cursor.fetchall()
# print("\nRegion Table:")
# for record in region_records:
#     print(record)

# cursor.execute("SELECT * FROM Bags")
# bags_records = cursor.fetchall()
# print("\nBags Table:")
# for record in bags_records:
#     print(record)

# Create Fact Table
create_sales_table_query = """
CREATE TABLE IF NOT EXISTS Sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    date_id INT,
    product_id INT,
    type_id INT,
    region_id INT,
    bags_id INT,
    average_price DECIMAL(10, 2),
    total_volume DECIMAL(10, 2),
    FOREIGN KEY (date_id) REFERENCES Date(date_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id),
    FOREIGN KEY (type_id) REFERENCES Type(type_id),
    FOREIGN KEY (region_id) REFERENCES Region(region_id),
    FOREIGN KEY (bags_id) REFERENCES Bags(bags_id)
);
"""

# Execute the query to create Sales (fact) table
cursor.execute(create_sales_table_query)

# Function to retrieve dimension IDs
def get_dimension_id(table_name, column_name, value):
    query = f"SELECT {column_name}_id FROM {table_name} WHERE {column_name} = %s"
    cursor.execute(query, (value,))
    result = cursor.fetchone()
    return result[0] if result else None

# Function to retrieve product_id and bags_id from their respective tables
def get_product_id(plu_codes):
    query = "SELECT product_id FROM Product WHERE plu_code_4046 = %s AND plu_code_4225 = %s AND plu_code_4770 = %s"
    cursor.execute(query, plu_codes)
    result = cursor.fetchone()
    return result[0] if result else None

def get_bags_id(bag_sizes):
    query = "SELECT bags_id FROM Bags WHERE small_bags = %s AND large_bags = %s AND xlarge_bags = %s"
    cursor.execute(query, bag_sizes)
    result = cursor.fetchone()
    return result[0] if result else None

# Read data from CSV file and insert into Sales table
with open('stagged_data.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        # Get dimension IDs
        date_id = get_dimension_id('Date', 'date', row['Date'])
        product_id = get_product_id((int(row['4046']), int(row['4225']), int(row['4770'])))
        type_id = get_dimension_id('Type', 'type', row['type_encoded'])
        region_id = get_dimension_id('Region', 'region', row['region'])
        bags_id = get_bags_id((int(row['Small Bags']), int(row['Large Bags']), int(row['XLarge Bags'])))

        # Check if all dimension IDs are valid
        if None not in (date_id, product_id, type_id, region_id, bags_id):
            # Insert data into Sales table
            insert_query = """
            INSERT INTO Sales (date_id, product_id, type_id, region_id, bags_id, average_price, total_volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                date_id, product_id, type_id, region_id, bags_id,
                row['AveragePrice'], row['Total Volume']
            ))


# View the fact table
# cursor.execute("SELECT * FROM Sales")
# sales_records = cursor.fetchall()
# print("\nSales Table:")
# for record in sales_records:
#     print(record)


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
# for row in roll_down_results:
#     print(row)

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

# Dice Query 1: Create a sub-cube for the year 2017 and New York City
dice_query_1 = """
SELECT
    city.city_name AS City,
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    City city ON s.city_id = city.city_id
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    YEAR(d.date) = 2017
    AND city.city_name = 'New York'
GROUP BY
    city.city_name, YEAR(d.date);
"""

# Execute the dice query 1
cursor.execute(dice_query_1)
dice_results_1 = cursor.fetchall()
# for row in dice_results_1:
#     print(row)

# Dice Query 2: Create a sub-cube for the month of January and Los Angeles
dice_query_2 = """
SELECT
    city.city_name AS City,
    MONTH(d.date) AS Month,
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    City city ON s.city_id = city.city_id
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    MONTH(d.date) = 1
    AND city.city_name = 'Los Angeles'
GROUP BY
    city.city_name, MONTH(d.date), YEAR(d.date);
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

# Query 2: OLAP operation combining slice and drill down - Analyzing total sales volume per city for the year 2018
# Slicing the data by year and drilling down to city level
city_sales_query = """
SELECT
    city.city_name AS City,
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    Date d ON s.date_id = d.date_id
JOIN
    City city ON s.city_id = city.city_id
WHERE
    YEAR(d.date) = 2018
GROUP BY
    city.city_name, YEAR(d.date);
"""

# Execute the query
cursor.execute(city_sales_query)
city_sales_records = cursor.fetchall()
# for record in city_sales_records:
#     print(record)

# Query 3: OLAP operation combining slice and dice - Analyzing total sales volume for a specific city in a particular month
# Slicing the data by year and drilling down to month level, then dicing for a specific city
specific_city_sales_query = """
SELECT
    city.city_name AS City,
    MONTH(d.date) AS Month,
    YEAR(d.date) AS Year,
    SUM(s.total_volume) AS TotalVolume
FROM
    Sales s
JOIN
    City city ON s.city_id = city.city_id
JOIN
    Date d ON s.date_id = d.date_id
WHERE
    YEAR(d.date) = 2018
    AND city.city_name = 'Chicago'  -- Specific city
GROUP BY
    city.city_name, MONTH(d.date), YEAR(d.date);
"""

# Execute the query
cursor.execute(specific_city_sales_query)
specific_city_sales_records = cursor.fetchall()
# for record in specific_city_sales_records:
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
#     print(record)

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()

print("Tables created and data inserted successfully.")
