-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls('/FileStore/tables')

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create table if not exists clinicaltrial_2021
-- MAGIC using csv
-- MAGIC options(
-- MAGIC header='true',
-- MAGIC delimiter='|',
-- MAGIC inferSchema='true',
-- MAGIC mode='FAILFAST',
-- MAGIC path='/FileStore/tables/clinicaltrial_2021.csv'
-- MAGIC );
-- MAGIC 
-- MAGIC cache table clinicaltrial_2021

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create table if not exists pharma(
-- MAGIC Company STRING,
-- MAGIC Parent_Company STRING,
-- MAGIC Subtraction STRING,
-- MAGIC Penalty_Amoun STRING,
-- MAGIC Penalty_Year STRING,
-- MAGIC Penalty_Date STRING,
-- MAGIC Offense_Group STRING,
-- MAGIC Primary_Offense STRING,
-- MAGIC Secondary_Offense STRING,
-- MAGIC Description STRING,
-- MAGIC Action_Type STRING,
-- MAGIC Agency STRING,
-- MAGIC Civil_Criminal STRING,
-- MAGIC Prosecution STRING,
-- MAGIC Court STRING,
-- MAGIC Case_ID STRING,
-- MAGIC Litigation STRING,
-- MAGIC Lawsuit STRING,
-- MAGIC Facility_State STRING,
-- MAGIC City STRING,
-- MAGIC Address STRING,
-- MAGIC Zip INT,
-- MAGIC NAICS_Code STRING,
-- MAGIC NAICS_Translation STRING,
-- MAGIC Country STRING,
-- MAGIC State STRING,
-- MAGIC Ownership_Structure STRING,
-- MAGIC Stock_Ticker STRING,
-- MAGIC Major_Industry STRING,
-- MAGIC Specific_Industry STRING,
-- MAGIC Info_Source STRING,
-- MAGIC Notes STRING)
-- MAGIC using csv OPTIONS (header="true", quoteChar "\"", ESCAPECHAR "\"", delimiter=",", path="/FileStore/tables/pharma.csv")

-- COMMAND ----------

create table if not exists mesh
using csv
options(
header='true',
delimiter=',',
inferSchema='true',
mode='FAILFAST',
path='/FileStore/tables/mesh.csv'
);

cache table mesh

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from clinicaltrial_2021
-- MAGIC limit 10;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from pharma
-- MAGIC limit 5;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from mesh
-- MAGIC limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 1

-- COMMAND ----------

select count(*) from clinicaltrial_2021

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 2

-- COMMAND ----------

select Type, count(*) as frequency
from clinicaltrial_2021
where Type is not null
group by Type
order by frequency desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 3 Using Explode function to split the diseases and naming the it new_disease

-- COMMAND ----------

--let explode split condition as C and clinicaltrial_2021 as trial--
select b.a as new_disease, count(*) as frequency
from (select explode(split(Conditions, ',')) as a from clinicaltrial_2021) b
group by new_disease
order by frequency desc
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 4

-- COMMAND ----------

--- Creating a view called diseases by exploding Conditions from clinical data---

create view if not exists diseases as
select  Clinical.C as new_disease
from (select explode(split(Conditions, ',')) as C from clinicaltrial_2021)  Clinical;

-- COMMAND ----------

---Selecting the first three elemrnt from the left of every record in tree column the join with view diseases and mesh---

select left(tree, 3) as codes, count(*) as frequency
from diseases di, mesh me
where me.term = di.new_disease
group by codes
order by frequency desc
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 5

-- COMMAND ----------

--to get sponsors from clinicaltrial 2021 data by creating view called Sponsor_table--

create view if not exists Sponsor_table as
select Sponsor
from clinicaltrial_2021;

-- COMMAND ----------

select * from Sponsor_table

-- COMMAND ----------

--to get pharmaceutical companies from pharma by creating a view called pharma_table--

create view if not exists Pharma_table as
select Parent_Company
from pharma;

-- COMMAND ----------

select * from Pharma_table

-- COMMAND ----------

--to get sponsors that are not pharmaceutical companies

create view if not exists sp as
SELECT * FROM Sponsor_table
WHERE Sponsor NOT IN ( SELECT Parent_Company FROM Pharma_table);

-- COMMAND ----------

select * from sp

-- COMMAND ----------

-- Counting sponsor--

select Sponsor, count(*) as  Total
from sp
where Sponsor is not null
group by Sponsor
order by  Total desc
limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 6

-- COMMAND ----------

-- select from left of completion column pick first 3 as month and from the right of Completion pick first 4 element--

select left(Completion, 3) as month, count(*) as frequency
from clinicaltrial_2021
where Status = 'Completed' and Completion !='' and right(Completion, 4) = '2021'
group by month
order by frequency desc;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC FURTHER ANALYSIS
-- MAGIC QUESTION: Count the number of Sponsor that are "Recruiting" and those "active,not recruiting"

-- COMMAND ----------

select Sponsor, count(Status)
from clinicaltrial_2021
where Status = "Recruiting"
group by Sponsor
limit 10;


-- COMMAND ----------

select Sponsor, count(Status)
from clinicaltrial_2021
where Status = "Active, not recruiting"
group by Sponsor
limit 10;
