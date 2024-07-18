-- Data Cleaning Project
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT *
FROM layoffs;

-- 1. Check for duplicates and remove any if found.
-- 2. Standardize the data and fix any errors.
-- 3. Null values or blank values.
-- 4. Remove any columns and rows that are not necessary.

-- First thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens.
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- 1. Remove Duplicates
-- First let's check for duplicates.

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- These are the ones we want to delete where the row number is greater than 1 or essentially 2 or greater.

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;

-- This throwsback an error because the target table duplicate_cte of the DELETE is not updatable.
-- One solution, which I think is a good one, is to create a new column and add those row numbers. Then, delete the rows where the row numbers are greater than 2, and finally, delete that column.

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

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Now that we have this we can delete rows were row_num is greater than 2.

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- 2. Standardize Data

-- I noticed that in a few company names there are unnecessary spaces.

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- ---------------------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto.

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Now that's taken care of:

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

-- ---------------------------------------------------------------

-- Everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Now if we run this again it is fixed.

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

-- ---------------------------------------------------------------

-- Let's also fix the date columns:

SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- We can use str to date to update this field.

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

-- Now we can convert the data type properly.

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Look at Null Values

-- If we look at industry it looks like we have some null and empty rows, let's take a look at these.

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Let's take a look at this.

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- It looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is:
-- Write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all.

-- We should set the blanks to nulls since those are typically easier to work with.

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT table1.industry, table2.industry
FROM layoffs_staging2 table1
INNER JOIN layoffs_staging2 table2
	ON table1.company = table2.company
    AND table1.location = table2.location
WHERE table1.industry IS NULL
AND table2.industry IS NOT NULL;

-- Now we need to populate those nulls if possible.

UPDATE layoffs_staging2 table1
INNER JOIN layoffs_staging2 table2
	ON table1.company = table2.company
SET table1.industry = table2.industry
WHERE table1.industry IS NULL
AND table2.industry IS NOT NULL;

-- And if we check it looks like Bally's was the only one without a populated row to populate this null values.

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- 4. Remove any columns and rows we need to

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use.

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;