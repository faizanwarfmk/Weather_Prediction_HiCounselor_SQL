use forecast;

create table weather 
( idx int primary key,       Dates VARCHAR(10),           temperature DOUBLE,    
Average_humidity  DOUBLE,    Average_dewpoint  DOUBLE,    Average_barometer  DOUBLE,  
Average_windspeed  DOUBLE,   Average_gustspeed  DOUBLE,   Average_direction  DOUBLE,  
Rainfall_for_month DOUBLE,   Rainfall_for_year DOUBLE,    Maximum_temperature  DOUBLE, 
Minimum_temperature  DOUBLE, Maximum_humidity DOUBLE,     Minimum_humidity DOUBLE,     
Maximum_pressure DOUBLE,     Minimum_pressure DOUBLE,     Maximum_windspeed  DOUBLE,  
Maximum_gust_speed DOUBLE,   Maximum_heat_index  DOUBLE,  Month INT,         diff_pressure DOUBLE );





-- 1. Give the count of the minimum number of days for the time when temperature reduced 
-- Min time period when temp reduced is 1 Day

   
  select min(time_period) as min_count_of_days from (
  select count(*) as time_period,identifiers from (  
   select *,dense_rank() over (order by dates) as ranks ,
   row_id - dense_rank() over (order by dates) as identifiers
   from (
   select dates, row_number() over (order by dates) as row_id,
		temperature,
		lead( temperature) over (order by dates) as next_day_temp
		from weather ) T1  where temperature > next_day_temp
					              ) T2
                       group by identifiers 
                       having count(*) =1
       ) master;
       
  
-- 2. Find the temperature as Cold / hot by using the case and avg of values of the given data set.

select Dates,
(case when temperature > avg_temp then 'Hot'
else 'Cold' end) as Cold_Hot
from(
		select Dates, temperature, 
		round(avg(temperature) over( ), 2) as avg_temp
		from weather ) sub;
        

-- 3. Can you check for all 4 consecutive days when the temperature was below 30 Fahrenheit


with CTE1 as (
			 select dates, temperature, 
			 row_number() over(order by dates) as row_id 
			 from weather
			 ) ,
	 CTE2 as ( 
		select * , dense_rank() over(order by dates) as ranks,
		row_id - dense_rank() over(order by dates) as identifier from CTE1
where temperature < 30) ,
	CTE3 as (select count(*) as 4days , identifier from CTE2 group by identifier
	having count(*)=4)
select * from CTE2 where identifier in ( select identifier from CTE3);

-- 4. Can you find the maximum number of days for which temperature dropped.

			
with CTE1 as ( select Dates, temperature, lead (temperature) over(order by dates) as next_day_temp,
			row_number() over(order by dates) as row_id 
			from weather
            ) ,
            
    CTE2 as (select *, dense_rank() over(order by Dates) , row_id - dense_rank() over(order by Dates) as group_id
    from CTE1  where temperature> next_day_temp)
    select max(total_days) as max_days_count_of_days from (select count(*) as total_days from cte2 group by group_id)
    sub;
    
    
    
-- 5. Can you find the average of average humidity from the dataset   
    
    select concat(mid( Dates,6,2), '_' , extract(year from Dates)) as YY_MM, 
    round(avg(average_humidity), 2) as avg_humidity
    from weather group by 1 order by Dates;
    
-- 6. Use the GROUP BY clause on the Date column and make a query to fetch details for average windspeed
-- ( which is now windspeed done in task 3 )    

select concat(mid( Dates,6,2), '_' , extract(year from Dates)) as YY_MM, 
    round(avg(average_windspeed), 2) as avg_windspeed
    from weather group by 1 order by Dates;
    
    

-- 7. Please add the data in the dataset for 2034 and 2035 as well as forecast predictions for these years 


create table forecast_data 
( idx int primary key,       Dates VARCHAR(10),           temperature DOUBLE,    
Average_humidity  DOUBLE,    Average_dewpoint  DOUBLE,    Average_barometer  DOUBLE,  
Average_windspeed  DOUBLE,   Average_gustspeed  DOUBLE,   Average_direction  DOUBLE,  
Rainfall_for_month DOUBLE,   Rainfall_for_year DOUBLE,    Maximum_temperature  DOUBLE, 
Minimum_temperature  DOUBLE, Maximum_humidity DOUBLE,     Minimum_humidity DOUBLE,     
Maximum_pressure DOUBLE,     Minimum_pressure DOUBLE,     Maximum_windspeed  DOUBLE,  
Maximum_gust_speed DOUBLE,   Maximum_heat_index  DOUBLE,  diff_pressure DOUBLE );



create table weather_forecast    select *  from (
				select * from weather 
				union 
				select * from forecast_data
			  ) sub ;
select *  from weather_forecast;  
select max(dates) from weather;
-- weather_forecast contains complete data

-- 8. If the maximum gust speed increases from 55mph, fetch the details for the next 4 days
 

with cte as (select Dates, Dates + interval 1 Day as 1Day,
			Dates + interval 2 Day as 2Day,
			Dates + interval 3 Day as 3Day,
			Dates + interval 4 Day as 4Day 
			from weather where Dates in( select  w.Dates 
			from weather w where w.Maximum_gust_speed >55))
 select sub.Date, w1.Maximum_gust_speed from (
 select Dates as Date from cte
 union 
 select 1Day as Date from cte
 union 
 select 2Day as Date from cte
 union 
 select 3Day as Date from cte
 union 
 select 4Day as Date from cte ) sub 
 join weather w1 
 on sub.Date=w1.Dates
 order by sub.Date;
 
-- alternate way
  
  with data as ( select rn, rn+1 as rn1, rn+2 as rn2,
   rn+3 as rn3, rn+4 as rn4
							from (
									select Dates,  Maximum_gust_speed , 
									row_number () over (order by Dates) as rn from weather) sub
									where Maximum_gust_speed >55),
 data2 as (
select rn from data
union
select rn1 from data
union	
select rn2 from data
union
select rn3 from data
union
 select rn4 from data)
select wx.Dates, wx.Maximum_gust_speed 
 from data2 join
 ( select Dates,  Maximum_gust_speed , 
row_number () over (order by Dates) as rn from weather) wx
on data2.rn =wx.rn 
order by 1;

 
 
-- 9. Find the number of days when the temperature went below 0 degrees Celsius 

select count(*) as total_days_below_0_deg from (select Dates , Minimum_temperature from weather 
where Minimum_temperature < 0) sub;

-- 10. Create another table with a “Foreign key” relation with the existing given data set. 

create table new_data_FK
( idx int ,                  Dates VARCHAR(10),           temperature DOUBLE,    
Average_humidity  DOUBLE,    Average_dewpoint  DOUBLE,    Average_barometer  DOUBLE,  
Average_windspeed  DOUBLE,   Average_gustspeed  DOUBLE,   Average_direction  DOUBLE,  
Rainfall_for_month DOUBLE,   Rainfall_for_year DOUBLE,    Maximum_temperature  DOUBLE, 
Minimum_temperature  DOUBLE, Maximum_humidity DOUBLE,     Minimum_humidity DOUBLE,     
Maximum_pressure DOUBLE,     Minimum_pressure DOUBLE,     Maximum_windspeed  DOUBLE,  
Maximum_gust_speed DOUBLE,   Maximum_heat_index  DOUBLE,  diff_pressure DOUBLE ,
foreign key (idx) references weather( idx) );

describe new_data_FK;


