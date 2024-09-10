-- Exploratory Data Analysis
SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

# total layoffs by companies within 3 years 2020 through 2023
SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

# total layoffs by company
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company 
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

# total layoffs by industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

# total layoffs by country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

# total layoffs by year 
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;
-- The number of layoffs is ramping up from 2022, strongest as of 2023. 
-- Within 3 months of records, layoffs are already at 125677 in 2023layoffs_staging2

# total layoffs by stage
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT * 
FROM layoffs_staging2;

# Total layoffs by date in ascending order
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

# Rolling layoffs total
WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,
SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

# Ranking company layoffs by year using CTE
WITH Company_Year (company, years, total_laid_off) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL;

# Ranking op 5 company layoffs by year (a CTE in a CTE)
WITH Company_Year (company, years, total_laid_off) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <= 5;

SELECT * 
FROM layoffs_staging2;

# TOP 10 companies with the highest percentage_laid_off, grouped by year and location
WITH Percent_Ranking (company, years, location, percentage_laid_off) AS
(
SELECT company, YEAR(`date`), location, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`), location
), location_ranking AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY percentage_laid_off DESC) AS Ranking
FROM Percent_Ranking
WHERE percentage_laid_off IS NOT NULL
)
SELECT * 
FROM location_ranking
WHERE Ranking <= 10 AND years IS NOT NULL;