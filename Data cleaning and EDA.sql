-- Data Cleaning 
select * from layoffs;
-- creating a database 
create table layoffs_staging like layoffs;
select * from layoffs_staging;
insert layoffs_staging select * from layoffs;
-- removing duplicates
select * ,row_number() over(partition by company,industry,total_laid_off,percentage_laid_off,`date`) from layoffs_staging;
-- The WITH Clause and CTE
-- WITH duplicate_cte AS (...):
-- This creates a Common Table Expression (CTE) named duplicate_cte. Think of it as a temporary result set you can refer to later in your SQL query.
-- 2. Selecting and Adding a Row Number
-- SELECT *, ...
-- You’re selecting all columns from the layoffs_staging table.
-- ROW_NUMBER() OVER (...):
-- This function assigns a sequential number (1, 2, 3, ...) to each row within each group specified by the PARTITION BY clause.
-- 3. The PARTITION BY Clause
-- PARTITION BY company, industry, total_laid_off, percentage_laid_off, date:
-- This means:Rows are grouped/partitioned based on having the same values for all of these columns:
-- company, industry, total_laid_off, percentage_laid_off, and date.
-- For each group of rows with identical values in these columns, ROW_NUMBER() starts at 1 and increments by 1 for each row.
-- 4. The row_num Column
-- The result is a new column called row_num that indicates the position of each row within its group (partition). 
-- The first matching row has row_num = 1, the second has row_num = 2, etc.
with duplicate_cte as(select * ,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,country,funds_raised_millions) as row_num from layoffs_staging)
-- displays the number of duplicates present in that country for example
select * from duplicate_cte where row_num>1;
-- casper has the 2 duplicate values
select * from layoffs_staging where company='Casper';
with duplicate_cte as(select * ,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,country,funds_raised_millions) as row_num from layoffs_staging)
-- we cannot delete directly 
delete 
from duplicate_cte 
where row_num>1;
-- creating another staging database for the deleting duplicates
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
insert into layoffs_staging2 select * ,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,country,funds_raised_millions) as row_num from layoffs_staging;
select*from layoffs_staging2;
delete from layoffs_staging2 where row_num>1;
select*from layoffs_staging2 where row_num>1;
-- standardizing data
select distinct(company) from layoffs_staging2;
select company,trim(company) from layoffs_staging2;
update layoffs_staging2 set company = trim(company);
select distinct(industry) from layoffs_staging2;
select distinct(industry) from layoffs_staging2 order by 1;
select*from layoffs_staging2 where industry like 'Crypto%';
-- we update the crypto values as crypto atlast crypto related words are same
update layoffs_staging2 set industry='Crypto' where industry like 'Crypto%';
select distinct location from layoffs_staging2 order by 1;
select distinct country from layoffs_staging2 order by 1;
select * from layoffs_staging2  where country like 'United States%' order by 1;
-- trim removes the extra space and by using trailing(search type) along with that we removed the '.' present in the country names
select distinct country,trim(trailing '.' from country) from layoffs_staging2 order by 1;
-- here now we updated the values of united states and also updated the '.' present united state like it created a single united states
update layoffs_staging2 set country=trim(trailing '.' from country) where country like 'United States%';
-- changing the data type of date as initially it is a text
select `date` from layoffs_staging2;
-- converting from string to dtae data type 
select `date` ,str_to_date(`date`,'%m/%d/%Y') from layoffs_staging2;
update layoffs_staging2 set `date`=str_to_date(`date`,'%m/%d/%Y');
alter table layoffs_staging2 modify column `date` DATE;
-- another column
-- removing null
select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
update layoffs_staging2 set industry =null where industry='';
select * from layoffs_staging2 where industry is null or industry = '';
select * from layoffs_staging2 where company = 'Airbnb';
-- here the industry column is null so we are goin to update it by its relative values
-- we perform join here
select * from layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company=t2.company  where (t1.industry is null or t1.industry='') and t2.industry is not null;
select t1.industry,t2.industry from layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company=t2.company  where (t1.industry is null or t1.industry='') and t2.industry is not null;
update layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company=t2.company 
set t1.industry=t2.industry where t1.industry is null and t2.industry is not null;
select * from layoffs_staging2 where company like 'Bally%';
update layoffs_staging2 set industry='Travel' where company like 'Bally%';
select industry from layoffs_staging2 where industry is null;
select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
-- deleting unnecessary data 
DELETE FROM layoffs_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
alter table layoffs_staging2 drop column row_num;
select*from layoffs_staging2;

-- 	EXPLORATORY DATA ANALYSIS
select max(total_laid_off),max(percentage_laid_off) from layoffs_staging2;
select * from layoffs_staging2 where percentage_laid_off=1 order by funds_raised_millions desc;
select company,sum(total_laid_off) from layoffs_staging2 group by company order by 2 desc;
select min(`date`),max(`date`) from layoffs_staging2;
select country,sum(total_laid_off) from layoffs_staging2 group by country order by 2 desc;
select industry,sum(total_laid_off) from layoffs_staging2 group by industry order by 2 desc;
select `date`,sum(total_laid_off) from layoffs_staging2 group by `date` order by 1 desc;
select year(`date`),sum(total_laid_off) from layoffs_staging2 group by year(`date`) order by 1 desc;
select stage,sum(total_laid_off) from layoffs_staging2 group by stage order by 2 desc;
select company,sum(percentage_laid_off) from layoffs_staging2 group by company order by 2 desc;
select stage,sum(percentage_laid_off) from layoffs_staging2 group by stage order by 2 desc;
-- rolling total
select substring(`date`,6,2)as `month`,sum(total_laid_off) from layoffs_staging2 group by `month`;
select substring(`date`,1,7)as `month`,sum(total_laid_off) from layoffs_staging2 where substring(`date`,1,7) is not null group by `month` order by 1 asc;
-- rolling tottal is defined as adding previous to present
with Rolling_Total as 
(select substring(`date`,1,7)as `month`,sum(total_laid_off) as total_off from layoffs_staging2 where substring(`date`,1,7) is not null group by `month` order by 1 asc)
select `month`,total_off,sum(total_off) over (order by `month`) from Rolling_Total;
select company,year(`date`),sum(total_laid_off) from layoffs_staging2 group by company,year(`date`) order by 3 desc;
with Company_Year (company,years,total_laid_off) as ( select company,year(`date`),sum(total_laid_off) from layoffs_staging2 group by company,year(`date`)),Company_Year_Rank as (
select*,dense_rank() over(partition by years order by total_laid_off desc) as Ranking from Company_Year where years is not null order by Ranking asc)select * from Company_Year_Rank where Ranking<=5;