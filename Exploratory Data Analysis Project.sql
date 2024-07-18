-- Exploratory Data Analysis Project

-- Here we are just going to explore the data and find trends, patterns, or anything interesting like outliers.
-- We will work with the table created in the 'Data Cleaning' project.

SELECT *
FROM layoffs_staging2;

-- EASIER QUERIES ---------------------------------------------------------------------------------------

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

-- Looking at percentage to see how big these layoffs were.

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off.

SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1;

-- If we order by funcs_raised_millions we can see how big some of these companies were.

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY------------------------------------------------------------

-- Companies with the biggest single layoff.

SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;

-- Companies with the most total layoffs.

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- By location.

SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC;

-- This it total in the past 3 years or in the dataset.

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 ASC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 ASC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- TOUGHER QUERIES---------------------------------------------------------------------------------------

-- Earlier we looked at companies with the most layoffs. Now let's look at that per year.

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

-- Rolling total of layoffs per month.

SELECT SUBSTRING(`date`,1,7) as `MONTH`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- Now use it in a CTE so we can query off of it.

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) as `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH` ASC) AS rolling_total
FROM Rolling_Total
ORDER BY `MONTH` ASC;