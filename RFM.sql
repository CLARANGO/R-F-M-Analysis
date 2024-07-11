--create table and import data
CREATE TABLE SALES_DATASET_RFM_PRJ
(
  ordernumber VARCHAR,
  quantityordered VARCHAR,
  priceeach        VARCHAR,
  orderlinenumber  VARCHAR,
  sales            VARCHAR,
  orderdate        VARCHAR,
  status           VARCHAR,
  productline      VARCHAR,
  msrp             VARCHAR,
  productcode      VARCHAR,
  customername     VARCHAR,
  phone            VARCHAR,
  addressline1     VARCHAR,
  addressline2     VARCHAR,
  city             VARCHAR,
  state            VARCHAR,
  postalcode       VARCHAR,
  country          VARCHAR,
  territory        VARCHAR,
  contactfullname  VARCHAR,
  dealsize         VARCHAR
) 


--create table and import data for RFM Segmentation Scores
CREATE TABLE segment_score
(
    segment Varchar,
    scores Varchar)
	
--Data cleaning

ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN customername TYPE text,
ALTER COLUMN productline TYPE text ,
ALTER COLUMN addressline1 TYPE text,
ALTER COLUMN addressline2 TYPE text,
ALTER COLUMN city TYPE text,
ALTER COLUMN state TYPE text,
ALTER COLUMN country TYPE text,
ALTER COLUMN territory TYPE text,
ALTER COLUMN orderlinenumber TYPE int USING (orderlinenumber::integer),
ALTER COLUMN quantityordered TYPE int USING (quantityordered::integer),
ALTER COLUMN ordernumber TYPE int USING (ordernumber::integer),
ALTER COLUMN msrp TYPE decimal USING (msrp::decimal),
ALTER COLUMN priceeach TYPE decimal USING (priceeach::decimal),
ALTER COLUMN sales TYPE decimal USING (sales::decimal) ;


UPDATE SALES_DATASET_RFM_PRJ
SET orderdate= to_timestamp(orderdate, 'mm/dd/yyyy HH24:MI');
ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN orderdate TYPE timestamp USING orderdate::timestamp WITHOUT time zone;


--Standardization
SELECT * FROM SALES_DATASET_RFM_PRJ;

UPDATE SALES_DATASET_RFM_PRJ
SET contactfullname = REPLACE(contactfullname,'-',' ');
UPDATE SALES_DATASET_RFM_PRJ
SET contactfullname = INITCAP(contactfullname);

--Check NULL

Select * FROM SALES_DATASET_RFM_PRJ
WHERE ordernumber IS NULL
OR quantityordered IS NULL
OR priceeach IS NULL
OR orderlinenumber IS NULL
OR sales IS NULL
OR orderdate IS NULL

--Check 0 value

Select * FROM SALES_DATASET_RFM_PRJ
WHERE ordernumber = '0'
OR quantityordered = '0'
OR priceeach ='0'
OR orderlinenumber = '0'
OR sales = '0'

--Check duplicate
SELECT * FROM (
SELECT *,
row_number() OVER(PARTITION BY ordernumber, orderlinenumber, orderdate) 
FROM SALES_DATASET_RFM_PRJ ) as row
WHERE row_number >1

--Remove outlier

WITH avg_sd AS (
SELECT quantityordered,
	(SELECT avg(quantityordered) as avg FROM SALES_DATASET_RFM_PRJ),
	(SELECT stddev(quantityordered) as sd FROM SALES_DATASET_RFM_PRJ)
FROM SALES_DATASET_RFM_PRJ),

sd_3 AS (
SELECT quantityordered, (quantityordered-avg)/sd as z_score
FROM avg_sd
WHERE ABS((quantityordered-avg)/sd)>3)

DELETE FROM SALES_DATASET_RFM_PRJ
WHERE quantityordered IN (SELECT quantityordered FROM sd_3)

-- Create new clean table after cleaning data
CREATE TABLE SALES_DATASET_RFM_PRJ_CLEAN AS 
(SELECT * FROM SALES_DATASET_RFM_PRJ )


----------------------------------------------------------------
--Calculate the RFM
WITH RFM AS (SELECT contactfullname,
current_date - MAX(orderdate) AS R,
COUNT(DISTINCT ordernumber) AS F,
SUM(sales) AS M
FROM SALES_DATASET_RFM_PRJ_CLEAN
WHERE status='Shipped'
GROUP BY contactfullname
ORDER BY contactfullname),

--divide rfm by scale 1-5
rfm_scale AS 
(SELECT contactfullname,
NTILE(5) OVER(ORDER BY R DESC) AS r_score,
NTILE(5) OVER(ORDER BY F) AS f_score,
NTILE(5) OVER(ORDER BY M) AS m_score
FROM RFM),

--reflect on corresponding criteria
rfm_fin AS (SELECT contactfullname,
r_score::varchar||f_score ::varchar||m_score::varchar as RFM
FROM rfm_scale)

SELECT segment,
COUNT(*) as total 
FROM(
	SELECT rfm_fin.contactfullname, s.segment
	FROM rfm_fin JOIN segment_score AS s ON rfm_fin.RFM=s.scores) 
AS a
GROUP BY segment
ORDER BY total






