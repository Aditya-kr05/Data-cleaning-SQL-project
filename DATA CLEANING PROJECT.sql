Create database world_layoffs;

select * 
from layoffs;


Create table layoffs_staging
like layoffs;

insert into layoffs_staging
select * 
from layoffs;

select *
From layoffs_staging;

select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions
	) as row_num
from 
layoffs_staging;

with duplicate_cte as 
(
select *,
row_number() over(partition by company, location, industry, total_laid_off,percentage_laid_off, `date`, stage, country,funds_raised_millions) as row_num
from 
layoffs_staging
)
select *
from duplicate_cte
where row_num >1;



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
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
From layoffs_staging2;


INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions
	) as row_num
from 
layoffs_staging;

select *
From layoffs_staging2
where row_num > 1;


Delete 
From layoffs_staging2
where row_num > 1;

select *
From layoffs_staging2;


-- standarizing data

select company , trim(company)
from layoffs_staging2;


update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2;



select distinct industry
from layoffs_staging2
order by 1;

select industry
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%'
;

select distinct country
from layoffs_staging2
order by 1;

select distinct country , trim(trailing '.' from  country)
from layoffs_staging2
order by 1;


update layoffs_staging2
set country =  trim(trailing '.' from  country);


select *
from layoffs_staging2
where company = 'airbnb';

update layoffs_staging2
set inudustry = null
where industry ='';



select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location =t2.location
where (t1.industry is null or t1.industry='')
and t2.industry is not null 
;
update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null 
;


delete 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null 
;


select *
from layoffs_staging2
where (total_laid_off is null or total_laid_off='')
and (percentage_laid_off is null or percentage_laid_off='')
;


select*
from layoffs_staging2;


-- now we not need any more the row column

ALTER table layoffs_staging2
DROP column row_num;
