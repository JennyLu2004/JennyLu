SELECT * 
FROM layoffs;

-- 1. REMOVE DUPLICATES
-- 2. STANDARDIZE THE DATA 
-- 3. NULL VALUES OR BLANK VALUES 
-- 4. REMOVE UNNECESSARY ROWS AND COLUMNS 

# Create a table that's a copy of the layoffs table
-- step 1: copy column names
CREATE TABLE layoffs_staging 
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

-- step 2: insert values from layoffs 
INSERT layoffs_staging
SELECT * 
FROM layoffs;

# REMOVING DUPLICATES 
# Create CTE
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
) 
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;

# It's good to check whether the rows with row_num > 1 are actually duplicates
SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';

# We want to delete the duplicates from the table, but the code below doesn't work because 
# a CTE is not updatable (deleting is updating) 
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
) 
DELETE
FROM duplicate_cte 
WHERE row_num > 1;

# Create a copy of the layoffs_staging table so we could delete the duplicates 
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
FROM layoffs_staging2;

# Insert values
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

# Filter to only duplicates 
SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

# Now, delete duplicates 
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

# Check
SELECT * 
FROM layoffs_staging2;

# STANDARDIZING DATA - find issues in your data and fix it
# 1. TRIMMING
SELECT company, TRIM(company)
FROM layoffs_staging2;

# Update the column company values to TRIM(company) values
UPDATE layoffs_staging2
SET company = TRIM(company);

# 2. Null values and similar meaning values
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

# Take a look at rows that starts with Crypto
SELECT * 
FROM layoffs_staging2 
WHERE industry LIKE 'Crypto%';

# Update all values starts with Crypto to 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto' 
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
 
# Somebody put a period after United States and there are two rows for United States 
SELECT * 
FROM layoffs_staging2 
WHERE country LIKE 'United States';

# Update country 
UPDATE layoffs_staging2 
SET country = 'United States'
WHERE country LIKE 'United States%';

# Or you can also do 
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) 
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country) 
WHERE country LIKE 'United States%';

# Formatting time series 
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') ;

SELECT `date`
FROM layoffs_staging2;

# This `date` column is still in text format, but now that we have the right format, we can convert it
# P.S Never do this on the raw table because it's gonna change the entire value format of your data
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging2;

# NULL AND BLANK VALUES 
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Look at rows where industry is null or blank
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

SELECT * 
FROM layoffs_staging2
WHERE company LIKE 'Airbnb';

# We want to update the blank value in Airbnb with Travel 
UPDATE layoffs_staging2 
SET industry = NULL 
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging2;

-- REMOVE UNNECESSARY COLUMNS AND ROWS 
# Before deleting data, you have to be confident that you don't need it or it's not affecting what you're looking for
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Drop column row_num 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

# Check cleaned table 
SELECT * 
FROM layoffs_staging2;
