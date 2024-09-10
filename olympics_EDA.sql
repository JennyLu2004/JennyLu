# OLYMPICS EDA PROJECT 
# Link to dataset on Kaggle: https://www.kaggle.com/datasets/heesoo37/120-years-of-olympic-history-athletes-and-results 

-- 1) How many olympics games have been held?
select count(distinct Games) as total_olympic_games
from athlete_events;

-- 2) List down all Olympics games held so far.
select distinct `Year`, Season, City 
from athlete_events
order by `Year`;

-- 3) Mention the total no of nations who participated in each olympics game?
with t1 as
	(select Games, region as Country
	from athlete_events AE
	join noc_regions NR
		on NR.NOC = AE.NOC
	group by Games, Country), t2 as 
    (select Games, count(1) as num_countries
    from t1
    group by Games)
select * 
from t2
group by Games
order by Games;

-- 4) Which year saw the highest and lowest no of countries participating in olympics?
select * 
from athlete_events;

# Practice
with t1 as 
	(select count(distinct Games) as total_games 
	from athlete_events), t2 as 
    (select Games, region as Country
    from athlete_events AE
    join noc_regions NR
		on NR.NOC = AE.NOC
	group by Games, Country), t3 as 
	(select Country, count(1) as total_participated_games
    from t2
    group by Country)
select Country, total_participated_games
from t3
join t1 
	on t1.total_games = t3.total_participated_games;
    
-- 6) Identify the sport which was played in all summer olympics.
# Find the number of summer olympic games 
# Find for each sport, how many sports where they played in 
# Compare 1st and 2nd 

# Practice 
select * 
from athlete_events;

with t1 as
	(select count(distinct Games) as total_summer_games 
	from athlete_events
    where Season = 'Summer'), t2 as 
    (select distinct Games, Sport 
    from athlete_events 
    where Season = 'Summer'), t3 as 
    (select Sport, count(1) as num_played
    from t2
    group by Sport)
select Sport, num_played
from t3
join t1 
	on t1.total_summer_games = t3.num_played;
    
-- 7) Which Sports were just played only once in the olympics?
with t1 as 
	(Select distinct Games, Sport
	from athlete_events), t2 as 
    (select Sport, count(1) as num_of_games
    from t1
    group by Sport) 
select t2.*, t1.Games
from t2
join t1 
	on t1.Sport = t2.Sport
where t2.num_of_games = 1
order by Sport;

-- 8) Fetch the total no of sports played in each olympic games.
with t1 as
	(select Sport, Games
	from athlete_events
    group by Sport, Games
    order by Games), t2 as 
    (select Games, count(1) as num_sports 
    from t1
    group by Games)
select Games, num_sports
from t2
order by num_sports desc;

-- 9) Fetch details of the oldest athletes to win a gold medal.
# Solution 1
with t1 as 
	(select distinct first_value(Age) over(order by Age desc) as Oldest
	from athlete_events
	where Age != 'NA' and Medal = 'Gold'), t2 as 
	(select *
	from athlete_events
	where Medal = 'Gold' and Age != 'NA')
select * 
from t2 
join t1 
	on t1.Oldest = t2.Age;
    
# Solution 2
with t1 as 
	(select *, 
	rank() over(order by Age desc) as rnk
	from athlete_events 
	where Medal = 'Gold' and Age != 'NA')
select * 
from t1
where rnk = 1;
-- 10) Find the Ratio of male and female athletes participated in all olympic games.
select * 
from athlete_events;

with t1 as 
	(select Sex, count(1) as cnt
	from athlete_events
	group by Sex), t2 as 
    (select *,
    row_number() over(order by cnt) as rnk 
    from t1), fcount as
    (select cnt
    from t2
    where rnk = 1), mcount as 
    (select cnt
    from t2
    where rnk = 2)
select concat('1 : ', round(mcount.cnt/fcount.cnt, 2)) as M_F_ratio -- round(a/b, 2) means round to 2 deci
from fcount, mcount;

select Sex, count(1) as cnt
from athlete_events
group by Sex;

-- 11) Fetch the top 5 athletes who have won the most gold medals.
# all athletes with gold medals
# number of gold medals each of these athletes won 
with t1 as
	(select Name, count(1) as gold_medals
	from athlete_events
	where Medal = 'Gold'
	group by Name
	order by 2 desc),
    t2 as 
    (select *, dense_rank() over(order by gold_medals desc) as rnk
    from t1)
select * 
from t2
where rnk < 6;

-- 12) Fetch the top 5 athletes who have won the most medals (gold/silver/bronze).
with t1 as 
	(select Name, Team, count(1) as total_medals
	from athlete_events
	where Medal in ('Gold', 'Silver', 'Bronze')
	group by Name, Team
	order by total_medals desc), t2 as 
    (select *, 
    dense_rank() over(order by total_medals desc) as rnk
    from t1) 
select Name, Team, total_medals
from t2
where rnk <6;

-- 13) Fetch the top 5 most successful countries in olympics. Success is defined by no of medals won.
with t1 as 
	(select region, count(1) as total_medals
	from athlete_events AE
	join noc_regions NR
		on NR.NOC = AE.NOC
	where Medal != 'NA'
	group by region
	order by total_medals desc), t2 as 
    (select *, 
    dense_rank() over(order by total_medals desc) as rnk
    from t1)
select * 
from t2
where rnk < 6;

-- 14) List down total gold, silver and broze medals won by each country.
select * 
from athlete_events;

select * 
from noc_regions;

# Practice 
with t1 as 
	(select region as Country, Medal, count(1) as total_medals
	from athlete_events AE
	join noc_regions NR
		on NR.NOC = AE.NOC
	where Medal != 'NA'
    group by Country, Medal
    order by Country, Medal)
select Country, 
SUM(CASE WHEN Medal = 'Gold' THEN total_medals ELSE 0 END) as Gold, 
SUM(CASE WHEN Medal = 'Silver' THEN total_medals ELSE 0 END) as Silver, 
SUM(CASE WHEN Medal = 'Bronze' THEN total_medals ELSE 0 END) as Bronze
from t1
group by Country
order by Gold desc, Silver desc, Bronze desc;

-- 15) List down total gold, silver and broze medals won by each country corresponding to each olympic games.
with t1 as
	(select Games, region as Country, Medal, Count(1) as total_medals
	from athlete_events AE
	join noc_regions NR
		on NR.NOC = AE.NOC
	where Medal != 'NA'
	group by Games, Country, Medal
	order by Country, Medal)
select Games, Country,
SUM(CASE WHEN Medal = 'Gold' THEN total_medals ELSE 0 END) as Gold,
SUM(CASE WHEN Medal = 'Silver' THEN total_medals ELSE 0 END) as Silver,
SUM(CASE WHEN Medal = 'Bronze' THEN total_medals ELSE 0 END) as Bronze
from t1
group by Games, Country
order by Games, Country;

-- 16) Identify which country won the most gold, most silver and most bronze medals in each olympic games.
select * 
from athlete_events;

select * 
from noc_regions;

# Practice 
with t1 as
	(select Games, region as Country, Medal, Count(1) as total_medals
	from athlete_events AE
	join noc_regions NR
		on NR.NOC = AE.NOC
	where Medal != 'NA'
	group by Games, Country, Medal
	order by Country, Medal), t2 as
	(select Games, Country,
	SUM(CASE WHEN Medal = 'Gold' THEN total_medals ELSE 0 END) as gold,
	SUM(CASE WHEN Medal = 'Silver' THEN total_medals ELSE 0 END) as silver,
	SUM(CASE WHEN Medal = 'Bronze' THEN total_medals ELSE 0 END) as bronze
	from t1
	group by Games, Country
	order by Games, Country)
select distinct Games,
	concat(first_value(Country) over(partition by Games order by gold desc), '- ',
	first_value(gold) over(partition by Games order by gold desc)) as max_gold,
	concat(first_value(Country) over(partition by Games order by silver desc), '- ', 
	first_value(silver) over(partition by Games order by silver desc)) as max_silver,
	concat(first_value(Country) over(partition by Games order by bronze desc), '- ',
	first_value(bronze) over(partition by Games order by bronze desc)) as max_bronze
from t2;

-- 17) Identify which country won the most gold, most silver, most bronze medals and the most medals in each olympic games.
with t1 as
	(select Games, region as Country, Medal, count(1) as total_medals
	from athlete_events AE
	join noc_regions NR
		on NR.NOC = AE.NOC
	where Medal != 'NA'
	group by Games, Country, Medal 
	order by Games, Country, Medal), t2 as
	(select Games, Country, total_medals,
	SUM(CASE WHEN Medal = 'Gold' THEN total_medals ELSE 0 END) as gold,
	SUM(CASE WHEN Medal = 'Silver' THEN total_medals ELSE 0 END) as silver,
	SUM(CASE WHEN Medal = 'Bronze' THEN total_medals ELSE 0 END) as bronze
	from t1
    group by Games, Country, total_medals
    order by Games, Country, gold desc, silver desc, bronze desc)
select distinct Games,
	concat(first_value(country) over(partition by games order by Gold desc), '- ',
	first_value(Gold) over(partition by games order by Gold desc)) as max_gold,
	concat(first_value(country) over(partition by games order by Silver desc), '- ',
	first_value(Silver) over(partition by games order by Silver desc)) as max_silver,
	concat(first_value(country) over(partition by games order by Bronze desc), '- ', 
	first_value(Bronze) over(partition by games order by Bronze desc)) as max_bronze,
    concat(first_value(country) over(partition by games order by total_medals desc), '- ',
    first_value(total_medals) over(partition by games order by total_medals desc)) as max_medals
from t2;

-- 18) Which countries have never won gold medal but have won silver/bronze medals?
# Practice
with t1 as
	(select region as Country,
	COUNT(CASE WHEN Medal = 'Gold' THEN 1 END) as gold,
	COUNT(CASE WHEN Medal = 'Silver' THEN 1 END) as silver, 
	COUNT(CASE WHEN Medal = 'Bronze' THEN 1 END) as bronze
	from athlete_events AE
	join noc_regions NR
		on NR.NOC = AE.NOC
	where Medal != 'NA'
	group by Country)
select * 
from t1
where gold = 0 and (silver > 0 or bronze > 0);

-- 19) In which Sport/event, India has won highest medals.
with t1 as 
	(select Sport, count(1) as total_medals
	from athlete_events 
	where Team = 'India' and Medal != 'NA'
	group by Team, Sport) 
select distinct
first_value(Sport) over(order by total_medals desc) as Sport, 
first_value(total_medals) over(order by total_medals desc) as highest_medal
from t1;

-- 20) Break down all olympic games where india won medal for Hockey and how many medals in each olympic games.
select Team, Sport, Games, count(1) as total_medals
from athlete_events
where Team = 'India' and Sport = 'Hockey'
group by Team, Sport, Games
order by Team, Sport, Games;
