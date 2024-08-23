create database sampl;
use sampl;

CREATE TABLE Car_sales (
    Car_id VARCHAR(15) PRIMARY KEY,
    Date DATE,
    Customer_Name VARCHAR(255) ,
    Gender VARCHAR(10),
    Annual_Income DECIMAL(10, 2),
    Company VARCHAR(255),
    Model VARCHAR(255),
    Engine VARCHAR(50),
    Transmission VARCHAR(50),
    Color VARCHAR(50),
    Price DECIMAL(10, 2),
    Body_Style VARCHAR(50),
    Phone VARCHAR(15),
    Region VARCHAR(10),
	Dealer_id INT,
    FOREIGN KEY(Dealer_id) REFERENCES car_dealers(Dealer_id)
);

LOAD DATA INFILE 'Car Sales.csv' INTO TABLE car_sales
FIELDS TERMINATED BY","
IGNORE 1 LINES;

## DATA EXPLORATION INTO COMPANY AND MODELS:
# What are the distinct car company and models?
SELECT DISTINCT(Company) from car_sales
ORDER BY COMPANY;

# Count of commpany cars
SELECT COUNT(DISTINCT Company) as total_company_cars from car_sales;

#Count of total models sold by the company:
SELECT COUNT(DISTINCT model) as total_models
from car_sales;

# How many models where present in each company?
SELECT Company,
	   Model,
       Count(Model) Over(Partition BY  Company ORDER BY Company) as count_of_model_present
       from car_sales
GROUP BY Company,
         Model;

#MODEL HAS BEEN WRITTEN IN SINGLE COLUMN SEPARTED BY COMMA
SELECT
    Company,
    GROUP_CONCAT(DISTINCT Model ) AS Models,
    COUNT(DISTINCT Model) AS count_of_models
FROM
    Car_sales
GROUP BY
    Company
ORDER BY count_of_models desc;
-------------------------------------------------------------------------------------------
# color-wise sales for each year
SELECT distinct(Color) from car_sales;

#colorwise sale differnce
SELECT
    EXTRACT(YEAR FROM Date) AS year_of_sale,
    Color,
    COUNT(*) AS sales_count,
    IFNULL(LAG(COUNT(*), 1) OVER (PARTITION BY Color ORDER BY EXTRACT(YEAR FROM Date)),0) as previous_sales,
    COALESCE(COUNT(*) - LAG(COUNT(*), 1) OVER (PARTITION BY Color ORDER BY EXTRACT(YEAR FROM Date)), 0) AS sales_difference
FROM
    car_sales
GROUP BY
    EXTRACT(YEAR FROM Date), Color
ORDER BY
    Color;
----------------------------------------------------------------------------
#Dealerwise total sales
SELECT COUNT(*) FROM car_sales;

SELECT Dealer_name,
	count(*) AS Total_sales
FROM
	car_dealers
JOIN car_sales USING(dealer_id)
group by
	Dealer_name
ORDER BY total_sales desc;

# Dealer Performnce: total companies,models sold by them and their total sales
SELECT d.dealer_name,
    COUNT( distinct s.model) as total_models,
    COUNT( distinct s.company) as total_companies,
    count(*) as total_sales
FROM car_dealers d
JOIN car_sales s USING(dealer_id)
GROUP BY d.dealer_name
order by total_sales desc,total_models desc;

#Discover which product has the highest sales for every dealer.
WITH CTE AS(
	SELECT d.DEALER_NAME,
		   COMPANY,
		   count(*) AS Total_sales,
		   DENSE_RANK() OVER(PARTITION BY Dealer_name order by count(*) desc) as rnk
	from car_sales c
    JOIN car_dealers d ON c.Dealer_Id = d.Dealer_Id
	GROUP BY 
		   d.DEALER_NAME,
		   COMPANY
	ORDER BY 
		   d.DEALER_NAME,
		   total_sales DESC)
SELECT DEALER_NAME,
	   COMPANY,
       Total_sales
       FROM CTE
WHERE rnk=1
order by total_sales desc;
# ===================
# the minimum and maximum prices of the Ford Company's Crown Victoria model across various dealers
WITH cte AS (
    SELECT
        d.Dealer_name,
        COMPANY,
        Model,
        c.dealer_region,
        ROUND(PRICE,0) AS price,
        MIN(ROUND(PRICE, 0)) OVER (PARTITION BY Dealer_name, Model) AS min_price,
       MAX(ROUND(PRICE,0)) OVER (PARTITION BY Dealer_name,Model) AS max_price
    FROM
        Car_sales c
    JOIN car_dealers d ON c.Dealer_Id = d.Dealer_Id
    WHERE
        COMPANY = 'FORD' AND Model='Crown Victoria'
    ORDER BY
        d.Dealer_name, Model, PRICE
)
SELECT
    *
FROM
    cte
ORDER BY
    dealer_region,price ;

#============================================
SHOW COLUMNS FROM car_sales

#ENGINEWISE SALES:
SELECT EXTRACT(YEAR FROM DATE) AS YEAR_OF_PURCHASE,
		ENGINE, 
        COUNT(*) AS TOTAL_SALES FROM car_sales
GROUP BY YEAR_OF_PURCHASE,
		 ENGINE
ORDER BY Total_sales;

# Preferred transmission by customers
SELECT Transmission,
       count(*) as total_sales
       from car_sales
       group by transmission
       order by total_sales desc;

#BODY STYLE WISE SALES
SELECT EXTRACT(YEAR FROM DATE) AS YEAR_OF_PURCHASE,
		Body_Style, 
        COUNT(*) AS Total_sales FROM car_sales 
group by YEAR_OF_PURCHASE,
		Body_Style
order by Year_of_purchase,Total_sales DESC;

# TOP 10 CARS SOLD IN EACH YEAR
WITH CTE AS(SELECT EXTRACT(YEAR FROM DATE) AS YEAR_OF_PURCHASE,
	   COMPANY,
       MODEL,
       ROW_NUMBER() OVER(PARTITION BY EXTRACT(YEAR FROM DATE) ORDER BY SUM(PRICE) DESC) AS RN,
       CAST(SUM(PRICE) AS signed) AS Amount
       from car_sales
GROUP BY YEAR_OF_PURCHASE,COMPANY,MODEL)
SELECT YEAR_OF_PURCHASE,
		COMPANY,
        MODEL,
        AMOUNT
        FROM CTE
WHERE RN<=10;

#CUSTOMER ANALYSIS:
SELECT GENDER,
		ROUND(SUM(PRICE),0) AS AMOUNT
        FROM car_sales
GROUP BY GENDER;

------------------------------------------------------------
# Calculate YOY Growth in Total Sales
WITH SalesData AS (
    SELECT
        EXTRACT(YEAR FROM Date) AS SaleYear,
        SUM(Price) AS TotalSales
    FROM
        CAR_sales
    GROUP BY
        EXTRACT(YEAR FROM Date)
)

SELECT
    s1.SaleYear AS CurrentYear,
    s1.TotalSales AS CurrentYearSales,
    s2.TotalSales AS PreviousYearSales,
	(s1.TotalSales - s2.TotalSales) AS YOYGROWTH,
    round(((s1.TotalSales - s2.TotalSales) / s2.TotalSales) * 100,2) AS YOYGrowthPercentage
FROM
    SalesData s1
JOIN
    SalesData s2 ON s1.SaleYear = s2.SaleYear + 1;

SELECT
    EXTRACT(YEAR FROM Date) AS SaleYear,
    SUM(Price) AS Amount,
    LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date)) AS Revenue_Previous_Year,
    SUM(Price) - LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date)) AS YOY_Difference,
    ROUND(((SUM(Price) - LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date))) /
    LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date))) * 100, 2) AS YOY_Growth_Percentage
FROM
    car_sales
GROUP BY
    SaleYear
ORDER BY
    SaleYear;

#MOM GROWTH
SELECT
    EXTRACT(YEAR FROM Date) AS SaleYear,
    EXTRACT(MONTH FROM Date) AS SaleMonth,
    SUM(Price) AS Amount,
    LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date), EXTRACT(MONTH FROM Date)) AS Revenue_Previous_Month,
    ROUND(SUM(Price) - LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date), EXTRACT(MONTH FROM Date)), 2) AS MoM_Difference,
    ROUND(((SUM(Price) - LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date), EXTRACT(MONTH FROM Date))) / 
       LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date), EXTRACT(MONTH FROM Date))) * 100, 2) AS MoM_Growth_Percentage
FROM
    car_sales
GROUP BY
    SaleYear, SaleMonth
ORDER BY
    SaleYear, SaleMonth;

# QUARTER OVER QUARTER GROWTH
SELECT
    EXTRACT(YEAR FROM Date) AS SaleYear,
    EXTRACT(quarter FROM Date) AS SaleMonth,
    SUM(Price) AS Amount,
    LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date),  EXTRACT(quarter FROM Date)) AS Revenue_Previous_Month,
    ROUND(SUM(Price) - LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date),  EXTRACT(quarter FROM Date)), 2) AS MoM_Difference,
    ROUND(((SUM(Price) - LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date),  EXTRACT(quarter FROM Date))) / 
       LAG(SUM(Price)) OVER (ORDER BY EXTRACT(YEAR FROM Date),  EXTRACT(quarter FROM Date))) * 100, 2) AS MoM_Growth_Percentage
FROM
    car_sales
GROUP BY
    SaleYear, SaleMonth
ORDER BY
    SaleYear, SaleMonth;


-- YOY SALES VOLUME
SELECT
    EXTRACT(YEAR FROM Date) AS SaleYear,
    count(Car_id) AS Amount,
    LAG(COUNT(Car_id)) OVER (ORDER BY EXTRACT(YEAR FROM Date)) AS Revenue_Previous_Year,
   count(Car_id) - LAG(count(Car_id)) OVER (ORDER BY EXTRACT(YEAR FROM Date)) AS YOY_Difference,
    ROUND(((count(Car_id) - LAG(count(Car_id)) OVER (ORDER BY EXTRACT(YEAR FROM Date))) /
    LAG(count(Car_id)) OVER (ORDER BY EXTRACT(YEAR FROM Date))) * 100, 2) AS YOY_Growth_Percentage
FROM
    car_sales
GROUP BY
    SaleYear
ORDER BY
    SaleYear;
