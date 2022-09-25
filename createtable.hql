--drop table
DROP TABLE IF EXISTS twitterdata;
--create table
CREATE EXTERNAL TABLE twitterData (id BIGINT, text STRING, lang STRING, retweetCount INT, favoriteCount INT, userId BIGINT, name STRING, screenName STRING, location STRING, followersCount INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar"=",", "quoteChar"="\"") LOCATION '/user/cloudera/hive_tables/all_tweets';
--show tables
SHOW TABLES;
