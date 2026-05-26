-- Databricks notebook source
-- Which artists released the most songs each year in 1990 or later?
SELECT artist_name, total_number_of_songs, year
  -- replace with the catalog/schema you are using:
  FROM playground.`getting-started`.top_artists_by_year
  WHERE year >= 1990
  ORDER BY total_number_of_songs DESC, year DESC;

-- COMMAND ----------

-- Find songs with a 4/4 beat and danceable tempo
SELECT artist_name, song_title, tempo
  -- replace with the catalog/schema you are using:
  FROM playground.`getting-started`.songs_prepared
  WHERE time_signature = 4 AND tempo between 100 and 140;
