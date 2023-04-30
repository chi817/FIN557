/*1 Download the data*/

/*Create a library */
libname project "/home/u63085035/fin557/project";

proc contents data=project.data;
run;

proc print data=project.data;
run;

data data;
set project.data;
run;

/*Calculate the total bankruptcy count for each year 2017-2023*/
proc sql;
select YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_year
from data
where LOC_STATE_COUNTRY='USA'
group by calculated YEAR;
quit;

/*Calculate the bankruptcy count for each SIC each year 2017-2023*/
proc sql;
select substr(SIC_CODE_FKEY,1,3) as SIC, 
       YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_year
from data
where LOC_STATE_COUNTRY='USA'
group by calculated SIC, calculated YEAR
order by calculated YEAR, bankruptcy_count_year desc;
quit;

proc sql;
select LOC_STATE as State, 
       YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_state
from data
where LOC_STATE_COUNTRY='USA'
group by State, calculated YEAR
order by calculated YEAR, bankruptcy_count_state desc;
quit;




/*Join the two tables*/

proc sql;
create table year_bank as
select YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_year
from data
where LOC_STATE_COUNTRY='USA'
group by calculated YEAR;
quit;

proc sql;
create table state_bank as
select LOC_STATE as State, YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_state
from data
where LOC_STATE_COUNTRY='USA'
group by State, calculated YEAR
order by calculated YEAR, bankruptcy_count_state desc;
quit;

proc sql;
select s.State, y.YEAR, s.bankruptcy_count_state, y.bankruptcy_count_year, 
       s.bankruptcy_count_state/y.bankruptcy_count_year as bankruptcy_pct format=PERCENT8.2
from year_bank y
full join state_bank s
on y.YEAR=s.YEAR;
quit;





proc contents data=project.data2;
run;

proc print data=project.data2 (obs=20);
run;

data data2;
set project.data2;
run;

proc sql;
select distinct TIC
from data2;
quit;

proc sql;
create table merge as
select *
from data d left join
     data2 d2
on d.BEST_EDGAR_TICKER=d2.TIC
where d2.NI is not null and 
      d2.DT is not null and 
      d2.ACT is not null and 
      d2.XINT is not null and 
      d2.TIC is not null and 
      d.BEST_EDGAR_TICKER is not null;
quit;

proc print data=merge;
run;