# Sparkify's Data Warehouse ETL process

## Summary

 - [Introduction](#introduction)
 - [Getting started](#getting-started)
 - [The ETL Process](#the-etl-process)
 - [Analyzing the results](#analyzing-the-results)
 - [The database structure](#the-database-structure)

## Introduction

This project uses Amazon Web Services S3 and Redshift to make the ETL process from raw log files of the Sparkify app to a database schema that provides analytical data to be queried.

## Getting started

Read the sections below to know how to get started:

### Configuration

First of all, you have to copy the `dwh.cfg.example` to a version without the example suffix (`dwh.cfg`), then fill all the fields of configuration. 

Letting only two fields empty: 
 - `HOST` (inside the `DB` configuration section) 
 - And the `ARN` (inside the `IAM_ROLE` configuration section) 

###  Infrastructure provisioning

There are **3 scripts** that will ease our job to create our data warehouse infrastructure:
#### 1. Creating a new AWS Redshift Cluster
```sh
python aws_create_cluster.py
```

#### 2. Checking the cluster availability 

_This one you should run several times until your cluster becomes available - takes from 3 to 6 minutes_

```sh
python aws_check_cluster_available.py
```

#### 3. Destroying the cluster 

_After the ETL process done, nor whenever you want, you can destroy it with a single command:_

```sh
python aws_destroy_cluster.py
```

## The ETL Process

It consists of these two simple python scripts:

 - `python create_tables.py` - It will drop the tables if exists, and then create it (again);
 - `python etl.py` - This script does two principal tasks:
     - Copy (load) the logs from the dataset's S3 bucket to the staging tables;
     - Translate all data from the staging tables to the analytical tables with `INSERT ... SELECT` statements.

## Analyzing the results

After the ETL process completion we can check if we did it right by running the `python analyze.py`.

It is a simple script to return the counting of each analytical table.

## The database structure

Below you can dive into the database structure (_a simple star schema_) created to run the analytical queries.

### Entity-relationship diagram

![DER](https://raw.githubusercontent.com/gabfr/data-engineering-nanodegree/master/2-cloud-data-warehouses/L4_Project_-_Data_Warehouse/data-warehouse-project-der-diagram.png) 

### Analytical Tables specifications

#### Song Plays table

- *Name:* `songplays`
- *Type:* Fact table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `songplay_id` | `INTEGER IDENTITY(0,1) SORTKEY` | The main identification of the table | 
| `start_time` | `TIMESTAMP NOT NULL` | The timestamp that this song play log happened |
| `user_id` | `INTEGER NOT NULL REFERENCES users (user_id)` | The user id that triggered this song play log. It cannot be null, as we don't have song play logs without being triggered by an user.  |
| `level` | `VARCHAR(10)` | The level of the user that triggered this song play log |
| `song_id` | `VARCHAR(20) REFERENCES songs (song_id)` | The identification of the song that was played. It can be null.  |
| `artist_id` | `VARCHAR(20) REFERENCES artists (artist_id)` | The identification of the artist of the song that was played. |
| `session_id` | `INTEGER NOT NULL` | The session_id of the user on the app |
| `location` | `VARCHAR(500)` | The location where this song play log was triggered  |
| `user_agent` | `VARCHAR(500)` | The user agent of our app |

#### Users table

- *Name:* `users`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `user_id` | `INTEGER PRIMARY KEY` | The main identification of an user |
| `first_name` | `VARCHAR(500) NOT NULL` | First name of the user, can not be null. It is the basic information we have from the user |
| `last_name` | `VARCHAR(500) NOT NULL` | Last name of the user. |
| `gender` | `CHAR(1)` | The gender is stated with just one character `M` (male) or `F` (female). Otherwise it can be stated as `NULL` |
| `level` | `VARCHAR(10) NOT NULL` | The level stands for the user app plans (`premium` or `free`) |


#### Songs table

- *Name:* `songs`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `song_id` | `VARCHAR(20) PRIMARY KEY` | The main identification of a song | 
| `title` | `VARCHAR(500) NOT NULL SORTKEY` | The title of the song. It can not be null, as it is the basic information we have about a song. |
| `artist_id` | `VARCHAR NOT NULL DISTKEY REFERENCES artists (artist_id)` | The artist id, it can not be null as we don't have songs without an artist, and this field also references the artists table. |
| `year` | `INTEGER NOT NULL` | The year that this song was made |
| `duration` | `NUMERIC (15, 5) NOT NULL` | The duration of the song |


#### Artists table

- *Name:* `artists`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist_id` | `VARCHAR(20) PRIMARY KEY` | The main identification of an artist |
| `name` | `VARCHAR(500) NOT NULL` | The name of the artist |
| `location` | `VARCHAR(500)` | The location where the artist are from |
| `latitude` | `DECIMAL(12,6)` | The latitude of the location that the artist are from |
| `longitude` | `DECIMAL(12,6)` | The longitude of the location that the artist are from |

#### Time table

- *Name:* `time`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `start_time` | `TIMESTAMP NOT NULL PRIMARY KEY` | The timestamp itself, serves as the main identification of this table |
| `hour` | `NUMERIC NOT NULL` | The hour from the timestamp  |
| `day` | `NUMERIC NOT NULL` | The day of the month from the timestamp |
| `week` | `NUMERIC NOT NULL` | The week of the year from the timestamp |
| `month` | `NUMERIC NOT NULL` | The month of the year from the timestamp |
| `year` | `NUMERIC NOT NULL` | The year from the timestamp |
| `weekday` | `NUMERIC NOT NULL` | The week day from the timestamp |

### Staging Tables specifications

The ETL process uses staging tables to copy the logs from unstructured log files to a single database table.

#### Events table

- *Name:* `staging_events`
- *Type:* Staging table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist` | `VARCHAR(500)` | The artist name |
| `auth` | `VARCHAR(20)` | The authentication status |
| `firstName` | `VARCHAR(500)` | The first name of the user |
| `gender` | `CHAR(1)` | The gender of the user |
| `itemInSession` | `INTEGER` | The sequence number of the item inside a given session |
| `lastName` | `VARCHAR(500)` | The last name of the user |
| `length` | `DECIMAL(12, 5)` | The duration of the song |
| `level` | `VARCHAR(10)` | The level of the user´s plan (free or premium) |
| `location` | `VARCHAR(500)` | The location of the user |
| `method` | `VARCHAR(20)` | The method of the http request |
| `page` | `VARCHAR(500)` | The page that the event occurred |
| `registration` | `FLOAT` | The time that the user registered |
| `sessionId` | `INTEGER` | The session id |
| `song` | `VARCHAR(500)` | The song name |
| `status` | `INTEGER` | The status |
| `ts` | `VARCHAR(50)` | The timestamp that this event occurred |
| `userAgent` | `VARCHAR(500)` | The user agent he was using |
| `userId` | `INTEGER` | The user id |

#### Songs table

- *Name:* `staging_songs`
- *Type:* Staging table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `num_songs` | `INTEGER` | The number of songs of this artist |
| `artist_id` | `VARCHAR(20)` | The artist id |
| `artist_latitude` | `DECIMAL(12, 5)` | The artist latitude location |
| `artist_longitude` | `DECIMAL(12, 5)` | The artist longitude location |
| `artist_location` | `VARCHAR(500)` | The artist descriptive location |
| `artist_name` | `VARCHAR(500)` | The artist name |
| `song_id` | `VARCHAR(20)` | The song id |
| `title` | `VARCHAR(500)` | The title |
| `duration` | `DECIMAL(15, 5)` | The duration of the song |
| `year` | `INTEGER` | The year of the song |

