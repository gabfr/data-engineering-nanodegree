# Sparkify's Data Lake ELT process

## Summary

 - [Introduction](#introduction)
 - [Getting started](#getting-started)
 - [Data sources](#data-sources)
 - [Parquet data schema](#parquet-data-schema)
 
## Introduction

This project aims to create analytical _parquet_ tables on Amazon S3 using AWS ElasticMapReduce/Spark to extract, 
load and transform songs data and event logs from the usage of the Sparkify app.

## Getting started

This ELT process is a pretty simple process. If it's your first time running this project, you should make a copy of the `dl.cfg.example` file, configure your AWS credentials and save it as `dl.cfg`.

Then, just run inside your Spark master machine: `python etl.py`

## Data sources

We will read basically two main data sources:

 - `s3a://udacity-dend/song_data/*/*/*` - JSON files containing meta information about song/artists data
 - `s3a://udacity-dend/log_data/*/*` - JSON files containing log events from the Sparkify app
 
 ## Parquet data schema
 
 After reading from these two data sources, we will transform it to the schema described below:
 
 #### Song Plays table

- *Location:* `s3a://social-wiki-datalake/songplays.parquet`
- *Type:* Fact table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `songplay_id` | `INTEGER` | The main identification of the table | 
| `start_time` | `TIMESTAMP` | The timestamp that this song play log happened |
| `user_id` | `INTEGER` | The user id that triggered this song play log. It cannot be null, as we don't have song play logs without being triggered by an user.  |
| `level` | `STRING` | The level of the user that triggered this song play log |
| `song_id` | `STRING` | The identification of the song that was played. It can be null.  |
| `artist_id` | `STRING` | The identification of the artist of the song that was played. |
| `session_id` | `INTEGER` | The session_id of the user on the app |
| `location` | `STRING` | The location where this song play log was triggered  |
| `user_agent` | `STRING` | The user agent of our app |

#### Users table

- *Location:* `s3a://social-wiki-datalake/users.parquet`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `user_id` | `INTEGER` | The main identification of an user |
| `first_name` | `STRING` | First name of the user, can not be null. It is the basic information we have from the user |
| `last_name` | `STRING` | Last name of the user. |
| `gender` | `STRING` | The gender is stated with just one character `M` (male) or `F` (female). Otherwise it can be stated as `NULL` |
| `level` | `STRING` | The level stands for the user app plans (`premium` or `free`) |


#### Songs table

- *Location:* `s3a://social-wiki-datalake/songs.parquet`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `song_id` | `STRING` | The main identification of a song | 
| `title` | `STRING` | The title of the song. It can not be null, as it is the basic information we have about a song. |
| `artist_id` | `STRING` | The artist id, it can not be null as we don't have songs without an artist, and this field also references the artists table. |
| `year` | `INTEGER` | The year that this song was made |
| `duration` | `DOUBLE` | The duration of the song |


#### Artists table

- *Location:* `s3a://social-wiki-datalake/artists.parquet`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist_id` | `STRING` | The main identification of an artist |
| `name` | `STRING` | The name of the artist |
| `location` | `STRING` | The location where the artist are from |
| `latitude` | `DOUBLE` | The latitude of the location that the artist are from |
| `longitude` | `DOUBLE` | The longitude of the location that the artist are from |

#### Time table

- *Name:* `s3a://social-wiki-datalake/time.parquet`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `start_time` | `TIMESTAMP` | The timestamp itself, serves as the main identification of this table |
| `hour` | `INTEGER` | The hour from the timestamp  |
| `day` | `INTEGER` | The day of the month from the timestamp |
| `week` | `INTEGER` | The week of the year from the timestamp |
| `month` | `INTEGER` | The month of the year from the timestamp |
| `year` | `INTEGER` | The year from the timestamp |
| `weekday` | `STRING` | The week day from the timestamp (Monday to Friday) |