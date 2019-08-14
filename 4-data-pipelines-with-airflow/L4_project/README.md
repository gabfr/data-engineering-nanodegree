# Sparkify's Event Logs Data Pipeline

## Introduction

This project consists of one Directed Acyclic Graph that implements the data pipeline responsible for reading all Sparkify's event
logs, process and create some fact/dimensions tables described in our data schema down below.

For illustration purposes you can check out the graph that represents this pipeline's flow:

![Directed Acyclic Graph of this Data Pipeline](https://raw.githubusercontent.com/gabfr/data-engineering-nanodegree/master/4-data-pipelines-with-airflow/L4_project/images/dag.png)

Briefly talking about this ELT process: 
 - Stages the raw data;
 - then transform the raw data to the songplays fact table;
 - and transform the raw data into the dimensions table too;
 - finally, check if the fact/dimensions table has at least one row.

## Data sources

We will read basically two main data sources on Amazon S3:

 - `s3://udacity-dend/song_data/` - JSON files containing meta information about song/artists data
 - `s3://udacity-dend/log_data/` - JSON files containing log events from the Sparkify app
 
## Data Schema

Besides the staging tables, we have 1 fact table and 4 dimensions table detailed below:

 #### Song Plays table

- *Table:* `songplays`
- *Type:* Fact table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `playid` | `varchar(32) NOT NULL` | The main identification of the table | 
| `start_time` | `timestamp NOT NULL` | The timestamp that this song play log happened |
| `userid` | `int4 NOT NULL` | The user id that triggered this song play log. It cannot be null, as we don't have song play logs without being triggered by an user.  |
| `level` | `varchar(256)` | The level of the user that triggered this song play log |
| `songid` | `varchar(256)` | The identification of the song that was played. It can be null.  |
| `artistid` | `varchar(256)` | The identification of the artist of the song that was played. |
| `sessionid` | `int4` | The session_id of the user on the app |
| `location` | `varchar(256)` | The location where this song play log was triggered  |
| `user_agent` | `varchar(256)` | The user agent of our app |

#### Users table

- *Table:* `users`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `userid` | `int4 NOT NULL` | The main identification of an user |
| `first_name` | `varchar(256)` | First name of the user, can not be null. It is the basic information we have from the user |
| `last_name` | `varchar(256)` | Last name of the user. |
| `gender` | `varchar(256)` | The gender is stated with just one character `M` (male) or `F` (female). Otherwise it can be stated as `NULL` |
| `level` | `varchar(256)` | The level stands for the user app plans (`premium` or `free`) |


#### Songs table

- *Table:* `songs`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `songid` | `varchar(256) NOT NULL` | The main identification of a song | 
| `title` | `varchar(256)` | The title of the song. It can not be null, as it is the basic information we have about a song. |
| `artistid` | `varchar(256)` | The artist id, it can not be null as we don't have songs without an artist, and this field also references the artists table. |
| `year` | `int4` | The year that this song was made |
| `duration` | `numeric(18,0)` | The duration of the song |


#### Artists table

- *Table:* `artists`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artistid` | `varchar(256) NOT NULL` | The main identification of an artist |
| `name` | `varchar(256)` | The name of the artist |
| `location` | `varchar(256)` | The location where the artist are from |
| `lattitude` | `numeric(18,0)` | The latitude of the location that the artist are from |
| `longitude` | `numeric(18,0)` | The longitude of the location that the artist are from |

#### Time table

- *Table:* `time`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `start_time` | `timestamp NOT NULL` | The timestamp itself, serves as the main identification of this table |
| `hour` | `int4` | The hour from the timestamp  |
| `day` | `int4` | The day of the month from the timestamp |
| `week` | `int4` | The week of the year from the timestamp |
| `month` | `varchar(255)` | The month of the year from the timestamp |
| `year` | `int4` | The year from the timestamp |
| `weekday` | `varchar(255)` | The week day from the timestamp (Monday to Friday) |
