.open Sharmo_db
.mode csv
.header on
.import Master.csv Master
.import Batting.csv Batting
.import Salaries.csv Salaries
.import Teams.csv Teams


select m.nameLast,m.nameFirst,m.playerID from Master m where m.playerID in(select b.playerID from Batting b,Salaries s where (b.playerID=s.playerID and b.yearID=2009 and s.yearID=2009) order by b.HR*1.0/s.salary DESC limit 1) limit 1;

select b.playerID,s.salary/b.HR*1.0 from Batting b,Salaries s where (b.playerID=s.playerID and b.yearID=2009 and s.yearID=2009) order by b.HR*1.0/s.salary DESC limit 1;

select t.teamID, avg(s.salary/t.HR) from Salaries s, Teams t where s.teamID = t.teamID and s.yearID=2008 and t.yearID=2008 group by t.teamID order by avg(s.salary/t.HR) DESC limit 1;

select t.teamID, avg(s.salary/t.HR) from Salaries s, Teams t where s.teamID = t.teamID and s.yearID=2008 and t.yearID=2008 group by t.teamID order by avg(s.salary/t.HR) DESC limit 1;
