🧹 World Layoffs — Data Cleaning (MySQL)
📘 Overview

This project cleans and standardizes global layoff data using SQL.
It follows a two-stage staging process to safely handle raw data and produce a clean, duplicate-free table for analysis.

⚙️ Steps

Create staging tables
layoffs_staging: raw copy of source
layoffs_staging2: working table with row_num for deduplication

Deduplicate
Used ROW_NUMBER() window function
Deleted rows where row_num > 1

Standardize Data
Trimmed spaces in company names

Unified industry labels (e.g., all “Crypto*” → “Crypto”)
Cleaned country names (United States. → United States)
Converted text dates to proper DATE type

Handle Missing Data
Imputed missing industries from the same company
Removed rows with no layoff info (total_laid_off and percentage_laid_off both NULL)

Final Cleanup
Dropped helper column row_num
Verified clean dataset ready for analysis

✅ Output
Table: world_layoffs.layoffs_staging2
Clean, deduplicated, and standardized dataset

Suitable for analytics and reporting

Author: Sujit Kumar Behera
