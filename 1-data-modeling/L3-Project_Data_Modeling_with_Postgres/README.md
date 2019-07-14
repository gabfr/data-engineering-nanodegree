# Sparkify song play logs ETL process

This project extract, transform and loads 5 main informations (tables) from the Sparkify app (an app to listen to your favorite musics) logs:
 - `users`
 - `songs`
 - `artists`
 - `songplays`
 - `time` - auxiliary table to help us breaks timestamps into comprehensible columns with time chunks (like `day`, `weekday`)

With this structured database we can extract several insightful informations from the way our users listens to their musics. Learning from its habits through hidden patterns inside this large quantity of data. 

Right down below you can read some instructions on how to create this database and then how to understand how this database was structured.

## Running the ETL

First you should create the PostgreSQL database structure, by doing:

```
python create_tables.py
```

Then parse the logs files:

```
python etl.py
```

## Database Schema Design

To learn how/why this schema design was made in this way, you should read our docs below: 

### Song Plays table

- *Name:* `songplays`
- *Type:* Fact table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `songplay_id` | `INTEGER` | The main identification of the table | 
| `start_time` | `TIMESTAMP NOT NULL` | The timestamp that this song play log happened |
| `user_id` | `INTEGER NOT NULL REFERENCES users (user_id)` | The user id that triggered this song play log. It cannot be null, as we don't have song play logs without being triggered by an user.  |
| `level` | `VARCHAR` | The level of the user that triggered this song play log |
| `song_id` | `VARCHAR REFERENCES songs (song_id)` | The identification of the song that was played. It can be null.  |
| `artist_id` | `VARCHAR REFERENCES artists (artist_id)` | The identification of the artist of the song that was played. |
| `session_id` | `INTEGER NOT NULL` | The session_id of the user on the app |
| `location` | `VARCHAR` | The location where this song play log was triggered  |
| `user_agent` | `VARCHAR` | The user agent of our app |

### Users table

- *Name:* `users`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `user_id` | `INTEGER PRIMARY KEY` | The main identification of an user |
| `first_name` | `VARCHAR NOT NULL` | First name of the user, can not be null. It is the basic information we have from the user |
| `last_name` | `VARCHAR NOT NULL` | Last name of the user. |
| `gender` | `CHAR(1)` | The gender is stated with just one character `M` (male) or `F` (female). Otherwise it can be stated as `NULL` |
| `level` | `VARCHAR NOT NULL` | The level stands for the user app plans (`premium` or `free`) |


### Songs table

- *Name:* `songs`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `song_id` | `VARCHAR PRIMARY KEY` | The main identification of a song | 
| `title` | `VARCHAR NOT NULL` | The title of the song. It can not be null, as it is the basic information we have about a song. |
| `artist_id` | `VARCHAR NOT NULL REFERENCES artists (artist_id)` | The artist id, it can not be null as we don't have songs without an artist, and this field also references the artists table. |
| `year` | `INTEGER NOT NULL` | The year that this song was made |
| `duration` | `NUMERIC (15, 5) NOT NULL` | The duration of the song |


### Artists table

- *Name:* `artists`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist_id` | `VARCHAR PRIMARY KEY` | The main identification of an artist |
| `name` | `VARCHAR NOT NULL` | The name of the artist |
| `location` | `VARCHAR` | The location where the artist are from |
| `latitude` | `NUMERIC` | The latitude of the location that the artist are from |
| `longitude` | `NUMERIC` | The longitude of the location that the artist are from |

### Time table

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

## The project file structure

We have a small list of files, easy to maintain and understand:
 - `sql_queries.py` - Where it all begins, this files is meant to be a query repository to use throughout the ETL process
 - `create_tables.py` - It's the file reponsible to create the schema structure into the PostgreSQL database
 - `etl.py` - It's the file responsible for the main ETL process
 - `etl.ipynb` - The python notebook that was written to develop the logic behind the `etl.py` process
 - `test.ipynb` - And finally this notebook was used to certify if our ETL process was being successful (or not).