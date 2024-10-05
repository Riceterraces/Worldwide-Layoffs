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


























