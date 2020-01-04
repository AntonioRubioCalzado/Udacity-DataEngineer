# Data Modeling with Postgres.
### 1. Introduction.

A startup called Sparkify wants to analyze all the collected data on songs and user activity on their new music streaming app. The company is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of CSV files on user activity on the app.

The goal of this project is to create an Apache Cassandra database with tables designed to make NoSQL analytics.

### 2. Available Data: Event Data.

The dataset we are working with in this project is **event_data**. The directory of CSV files **/event_data/data/** is partitioned by date. An example of filepath in this dataset would be `event_data/2018-11-08-events.csv`.


### 3. Tables.

The model defined with the previous data is a Star Model composed of the following tables:

| TABLE | DESCRIPTION | FIELDS | PRIMARY KEY |
|----------------------------------|----------------------------------------------------------|---------------------------------------------------------------------|--------------------------------------------------------------------|
| session_history | Information of artists and songs of a particular session | artist, song, sessionId, itemInSession, firstName, lastName, userId | sessionId (Partition Key), itemInSession (Clustering Key) |
| song_for_user | Information of songs for a particular user | user_id, first_name, last_name, gender, level | userId, sessionId (Partition Key), itemInSession (Clustering Key)| |
| user_for_song| Information of users for a particular song | firstName, lastName, song_id, userId | song (Partition Key), userId (Clustering Key) |

### 4. Execution order of the code.

The only code in this project is on the Jupyter notebook `Project_1B_ Project_Template.ipynb`. To run it, a Python 3.7 kernel must be turned on and code must be run cell by cell on the order of the cells (to avoid problems with dependence).