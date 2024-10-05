## Data Cleaning 


SELECT * 
FROM layoffs;

## 1.Remove Duplicates 
## 2.Standardize the Data 
## 3.Null Values or blank values 
## 4.Remove Any Columns 



CREATE TABLE layoffs_staging 
LIKE layoffs ;


SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
industry,total_laid_off,percentage_laid_off,`date`,stage
,country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;


SELECT *
FROM layoffs_staging
WHERE company='Casper';

















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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num>1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
industry,total_laid_off,percentage_laid_off,`date`,stage
,country, funds_raised_millions) AS row_num
FROM layoffs_staging;



DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


## Standardizing the Data 

SELECT company , TRIM(company)
FROM layoffs_staging2;	

UPDATE layoffs_staging2
SET company =TRIM(company);

 
SELECT DISTINCT industry
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry Like 'Crypto%';



SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'Unites States%';

SELECT `date`
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET `date` =STR_TO_DATE(`date`,' %m/%d/%Y');


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry= NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Ball%';

SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL ;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2;



SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

##Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;


SELECT MAX(total_laid_off), MAX(percentage_laid_off) 
FROM layoffs_staging2 ;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off=1         # Companies completely laid off
ORDER BY total_laid_off DESC ;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company                 # total laid off for each company 
ORDER BY 2 desc ;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;              # date that has a minimum and maximum laid off


SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2                   # Total laid off of each industry 
GROUP BY industry
ORDER BY 2 desc ;

SELECT *
FROM layoffs_staging2;

SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2                       # Total laid off each year 2020-2023
GROUP BY YEAR( `date`)
ORDER BY 1 DESC;


SELECT stage,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage                            #Total laid off each stage   
ORDER BY 1 DESC;


SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2                          #Average laid off per company
GROUP BY company
ORDER BY 2 desc ;




SELECT SUBSTRING(`date`,1,7)   AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7)  IS NOT NULL       # Total Laid off each month 
group by `MONTH`
ORDER BY 1 ASC;


WITH Rolling_Total As 
(
SELECT SUBSTRING(`date`,1,7)  AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7)  IS NOT NULL    # SUBSTRING(`date`,1,7) 
group by `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off 
,SUM(total_off) OVER(ORDER  BY `MONTH`) AS rolling_total 
FROM Rolling_Total;



SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 desc ;


SELECT company,	YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,`date`
ORDER BY 3 DESC ;

WITH Company_Year(company,years,total_laid_off) AS 
(
SELECT company,	YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
),Company_Year_Rank AS
(SELECT * ,
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking<=5
;



























