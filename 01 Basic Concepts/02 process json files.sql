-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC JSON files are available under "datafiles" sub folder

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Process Simple JSON Document

-- COMMAND ----------

select * from json.`/FileStore/datasets/02_bored.json`;

-- COMMAND ----------

select * from json.`/FileStore/datasets/02_bored_pretty.json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create table using Json (Pretty)

-- COMMAND ----------

create table bored_bronze_json using json
options(path='/FileStore/datasets/02_bored_pretty.json' , multiline=true);

-- COMMAND ----------

select * from bored_bronze_json;

-- COMMAND ----------

describe bored_bronze_json;

-- COMMAND ----------

describe extended bored_bronze_json;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Nested JSON

-- COMMAND ----------

select * from json.`/FileStore/datasets/02_albums.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create External Table

-- COMMAND ----------

create table albums_bronze_json using json
options(path='/FileStore/datasets/02_albums.json' , multiline=true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Query the Table

-- COMMAND ----------

select * from albums_bronze_json

-- COMMAND ----------

select feed.author from albums_bronze_json;

-- COMMAND ----------

select feed.author.name, feed.author.url from albums_bronze_json;

-- COMMAND ----------

select feed.author.name as author_name, feed.author.url as author_url, feed.country, feed.results from albums_bronze_json;

-- COMMAND ----------

select feed.author.name as author_name, feed.author.url as author_url, feed.country, explode(feed.results) from albums_bronze_json;

-- COMMAND ----------

select feed.author.name as author_name, feed.author.url as author_url, feed.country, lv_results from albums_bronze_json
lateral view explode(feed.results) as lv_results;


-- COMMAND ----------

select feed.author.name as author_name, feed.author.url as author_url, feed.country, lv_results.* from albums_bronze_json
lateral view explode(feed.results) as lv_results;

-- COMMAND ----------

select feed.author.name as author_name, feed.author.url as author_url, feed.country, lv_results.*, lv_genres.url as gen_url from albums_bronze_json
lateral view explode(feed.results) as lv_results
lateral view explode(lv_results.genres) as lv_genres

-- COMMAND ----------

create table albums_bronze_delta as
select feed.author.name as author_name, feed.author.url as author_url, feed.country, lv_results.*, lv_genres.url as gen_url from albums_bronze_json
lateral view explode(feed.results) as lv_results
lateral view explode(lv_results.genres) as lv_genres;

-- COMMAND ----------

select * from albums_bronze_delta;

-- COMMAND ----------

select feed.author.* from albums_bronze_json;

-- COMMAND ----------


