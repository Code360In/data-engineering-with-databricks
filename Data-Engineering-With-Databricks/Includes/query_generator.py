# Databricks notebook source
class QueryGenerator:
    def __init__(self, course, mode="normal"):
        import re
        import random
        self.username = spark.sql("SELECT current_user()").first()[0]
        self.userhome = f"dbfs:/user/{self.username}/{course}"
        self.database = f"""dbacademy_{re.sub("[^a-zA-Z0-9]", "_", self.username)}_{course}"""

        if mode == "reset":
            spark.sql(f"DROP DATABASE IF EXISTS {self.database} CASCADE")
            dbutils.fs.rm(self.userhome, True)

    def config(self):
        print(f"""
CREATE DATABASE {self.database}
LOCATION '{self.userhome}';

USE {self.database};
    
CREATE TABLE user_ping 
(user_id STRING, ping INTEGER, time TIMESTAMP); 

CREATE TABLE user_ids (user_id STRING);

INSERT INTO user_ids VALUES
("potato_luver"),
("beanbag_lyfe"),
("default_username"),
("the_king"),
("n00b"),
("frodo"),
("data_the_kid"),
("el_matador"),
("the_wiz");

CREATE FUNCTION get_ping()
    RETURNS INT
    RETURN int(rand() * 250);
    
CREATE FUNCTION is_active()
    RETURNS BOOLEAN
    RETURN CASE 
        WHEN rand() > .25 THEN true
        ELSE false
        END;
        """)
        
    def load(self):
        print(f"""
INSERT INTO {self.database}.user_ping
SELECT *, 
  {self.database}.get_ping() ping, 
  current_timestamp() time
FROM {self.database}.user_ids
WHERE {self.database}.is_active()=true;

SELECT * FROM {self.database}.user_ping;
        """)
        
    def user_counts(self):
        print(f"""
SELECT user_id, count(*) total_records
FROM {self.database}.user_ping
GROUP BY user_id
ORDER BY 
  total_records DESC,
  user_id ASC;;
        """)
        
    def avg_ping(self):
        print(f"""
SELECT user_id, window.end end_time, mean(ping) avg_ping
FROM {self.database}.user_ping
GROUP BY user_id, window(time, '3 minutes')
ORDER BY
  end_time DESC,
  user_id ASC;
        """)

    def summary(self):
        print(f"""
SELECT user_id, min(time) first_seen, max(time) last_seen, count(*) total_records, avg(ping) total_avg_ping
FROM {self.database}.user_ping
GROUP BY user_id
ORDER BY user_id ASC
        """)

