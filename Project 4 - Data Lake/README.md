# Data Lake
## Introduction

A music streaming startup, Sparkify, has grown their user base and song database
even more and want to move their data warehouse to a data lake. Their data resides
in S3, in a directory of JSON logs on user activity on the app, as well as a
directory with JSON metadata on the songs in their app.

The project is about building an ETL pipeline that extracts their data from S3,
processes them using Spark, and loads the data back into S3 as a set of dimensional
tables. This will allow their analytics team to continue finding insights in what
songs their users are listening to.

## Database schema design

The Star Data Mart Schema is implemented in this ETL pipeline. Concretely, there
is 1 fact table `Songplays` that keeps users' music listening events. Surrounding
the fact table are 4 dimension tables `Users`, `Artists`, `Songs` and `Time`,
which store entity details for further retrieval by means of `JOIN` operator. The
figure below best describes the model's architecture:

<img src="./images/Music Streaming ERD.png" width="800" height="600">

With the above architecture, you can easily query basic information of an event
directly from `Songplays`. Moreover, you can get the event's details, such as
user information or listened song information, simply by joining the fact table
with the corresponding dimension table.

## Structure of S3 buckets
### `dtl-scripts` S3 bucket

`dtl-scripts` keeps two scripts: `etl.py` and `dtl.cfg`, which will be submitted
to the EMR cluster via `spark-submit`.

Where:

1. `etl.py` contains the script to perform the ETL pipeline.
2. `dtl.cfg` contains necessary configuration information that is used by `etl.py`.

### `source-dtl` S3 bucket

The structure of `source-dtl` is as follow:
```
source-dtl
│
└───log_data
│   └───$year_of_log
│   │   └───$month_of_log
│   │   │   │   $log_data.json
│   │   │   │   ...
│   │   │
│   │   └───...
│   │
│   └───...
│
└───song_data
    └───$1st_letter_trackid
    │   └───$2nd_letter_trackid
    │   │   └───$3rd_letter_trackid
    │   │   │   │   $song_data.json
    │   │   │   │   ...
    │   │   │
    │   │   └───...
    │   │
    │   └───...
    │
    └───...
```

Where:

1. `song_data` contains a subset of real data from the Million Song Dataset.
Each file is in JSON format and contains metadata about a song and the artist
of that song. The files are partitioned by the first three letters of each
song's track ID. And a song data JSON file looks like this.
    ```
    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
    ```
2. `log_data` consists of log files in JSON format generated by this event
simulator based on the songs in the dataset above. These simulate activity
logs from a music streaming app based on specified configurations. The log
files in the dataset are partitioned by year and month. And below is an
example of what the data in a log file looks like.
<img src="./images/Log data.png" width="1000" height="300">

### `target-dtl` S3 bucket

The structure of `target-dtl` is as follow:
```
target-dtl
│
└───artists
│   └───$artists_partition.parquet
│       │
│       └───...
│
└───songplays
│   └───$songplays_partition.parquet
│       │
│       └───...
│
└───songs
│   └───$songs_partition.parquet
│       │
│       └───...
│
└───time
│   └───$time_partition.parquet
│       │
│       └───...
│
└───users
    └───$users_partition.parquet
        │
        └───...
```

Where each folder contains partitions of the corresponding table as parquet files.

## Instructions
### Prerequisites

These are the prerequisites to run the codes:
* `dtl-scripts` and `source-dtl` with the structure specified in the above section.
* A cluster with the following settings:
    * Release: `emr-5.20.0` or later
    * Applications: Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and
    Zeppelin 0.8.0
    * Instance type: `m3.xlarge`
    * Number of instance: `3`
    * EC2 key pair: Use a key pair generated using `EC2` service.

### How to run

1. SSH to the EMR cluster.
2. Run the following command:

```
/usr/bin/spark-submit --master yarn s3a://dtl-scripts/etl.py
```

3. Now you've got a complete Data Lake. You can query and visualize the data, or
check for the database constraints using EMR Notebook.


## License
Distributed under the MIT License. [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)