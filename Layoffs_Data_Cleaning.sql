-- =========================================================
-- Project: World Layoffs — Data Cleaning Pipeline (MySQL)
-- Purpose: Create a clean, standardized, de-duplicated table
-- Inputs: world_layoffs.layoffs (raw source)
-- Outputs: world_layoffs.layoffs_staging2 (cleaned data)
-- Notes:
--   - Uses a two-stage staging approach:
--       1) layoffs_staging: raw copy to preserve source
--       2) layoffs_staging2: working table with row_num for deduplication
--   - Standardizes company, industry, country, and date formats
--   - Imputes industry using same-company records where possible
--   - Removes records with no layoff info (both total and percentage NULL)
-- =========================================================

USE world_layoffs;

-- 1) Create a raw staging table identical to source to avoid mutating the original
CREATE TABLE layoff_staging
LIKE world_layoffs.layoffs;

-- Quick check of structure/data (safe preview)
SELECT * FROM world_layoffs.layoff_staging;

-- Load raw data into first-stage staging
INSERT layoffs_staging
SELECT * FROM layoffs;

-- 2) Explore potential duplicates using a window function over key business fields
--    Adjust PARTITION BY columns if business logic changes
SELECT *,
       ROW_NUMBER() OVER(
         PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
       ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Optional: broader duplicate key including more attributes
WITH duplicate_cte AS (
  SELECT *,
         ROW_NUMBER() OVER(
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
                        `date`, stage, country, funds_raised_millions
         ) AS row_num
  FROM world_layoffs.layoffs_staging
)
SELECT * FROM duplicate_cte WHERE row_num > 1;

-- Spot-check a specific company when investigating duplicates/anomalies
SELECT * FROM world_layoffs.layoffs_staging
WHERE company = 'Casper';

-- 3) Create a second staging table with a row_num column for deterministic deduplication
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

USE world_layoffs;

-- Safety check: ensure row_num column exists for dedupe logic
SELECT * FROM layoffs_staging2
WHERE world_layoffs.layoffs_staging2.row_num > 1;

-- Populate layoffs_staging2 and compute row_num for duplicates
INSERT INTO world_layoffs.layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER(
         PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
                      `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Remove duplicate rows (keep the first instance where row_num = 1)
DELETE FROM world_layoffs.layoffs_staging2
WHERE world_layoffs.layoffs_staging2.row_num > 1;

-- =========================
-- Standardization Section
-- =========================

-- Trim company names to remove leading/trailing whitespace
SELECT world_layoffs.layoffs_staging2.company, TRIM(world_layoffs.layoffs_staging2.company)
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET world_layoffs.layoffs_staging2.company = TRIM(world_layoffs.layoffs_staging2.company);

-- Normalize industry labels: collapse all “Crypto*” variants into “Crypto”
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE world_layoffs.layoffs_staging2.industry LIKE 'Crypto%';

UPDATE world_layoffs.layoffs_staging2
SET world_layoffs.layoffs_staging2.industry = 'Crypto'
WHERE world_layoffs.layoffs_staging2.industry LIKE 'Crypto%';

-- Standardize country names (e.g., remove trailing periods in “United States.”)
-- TRIM(TRAILING '.' ...) safely strips trailing dots without affecting acronyms
SELECT DISTINCT world_layoffs.layoffs_staging2.country,
       TRIM(TRAILING '.' FROM world_layoffs.layoffs_staging2.country) AS cntry
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

UPDATE world_layoffs.layoffs_staging2
SET world_layoffs.layoffs_staging2.country = TRIM(TRAILING '.' FROM country)
WHERE world_layoffs.layoffs_staging2.country LIKE 'United States%';

-- =========================
-- Date Parsing and Casting
-- =========================

-- Preview parsed date to validate format mapping
SELECT `date`,
       STR_TO_DATE(`date`, '%m/%d/%Y') AS formatted_date
FROM world_layoffs.layoffs_staging2;

SELECT * FROM world_layoffs.layoffs_staging2;

-- Convert text date to proper DATE type (assuming MM/DD/YYYY in source)
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Enforce DATE column type at schema level
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * FROM world_layoffs.layoffs_staging2;

-- =========================
-- Null/Empty Handling
-- =========================

-- Identify records with no layoff metrics (potentially non-informative rows)
SELECT * FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Normalize empty strings to NULL for industry to simplify downstream filters
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Audit remaining missing industries
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
   OR industry = '';

-- Company-level inspection for targeted fixes (example: Airbnb)
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Airbnb';

-- Impute missing industry by joining to another row of the same company with a known industry
SELECT t1.industry, t2.industry
FROM world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
  ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;

UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Hard-fix known outliers after audit (document rationale: Airbnb belongs to Travel)
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Travel'
WHERE company = 'Airbnb' AND (industry IS NULL OR industry = '');

-- Re-check rows that lack both layoff metrics (these are typically safe to drop)
SELECT * FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Remove rows with no layoff data, as they do not contribute to analysis
DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Final verification of cleaned dataset
SELECT *
FROM world_layoffs.layoffs_staging2;

-- Drop helper column now that deduplication is complete
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;
