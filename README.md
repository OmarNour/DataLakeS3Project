## Project Summary:
-   **The Goal**: helping Sparkify move their user and song to a data lake due to the increasing amount of data.
-   **Datasets**: the data resides in S3, in a directory of json logs
-   **database layers**:
    1. **Source Data**: Json files in S3.
    2.  **DWH layer**: moveing data from json to arquet files in S3 too using spark, to be able to transform and make join between tables easily.

## Files in the repository:
1. **dl.cfg**: obtains configuration needed to populate the DWH db successfully
2. **etl.py**: The main processes:
    1. Process song data, to populate songs and artists tables
    2. Process log data, to populate users and time tables.
    3. Populate songplays fact table by joining song and log data. 

## How to run the python scripts:
1. Make sure configured dl.cfg
2. run etl.py

## The purpose of this database:
To help Sparkify analyze data they are collecting on songs and user activities  
by building a star schema on parquet files that serves their needs.  

The goal is to understand what songs users are listening to!

## Database Schema Design & ETL Pipeline:
-   **database layers**:
    1. **Staging layer**: this is almost one to one from S3 to be able to transform and make join between tables easily.
    2.  **DWH layer**: here we have our data transformed ans sored in a star schema, ready to answer questions.
      
        - **Staging tables**:
            1. **staging_events**: a copy from event log files,  
                and because of the number of rows is small so we choose to make the distribution style "ALL".  
            2. **staging_songs**: a copy from song log files
        - **Fact table**:
            1. **songplays**:
                1. songplay_id      (Auto increment integer)
                2. start_time
                3. user_id
                4. level
                5. song_id
                6. artist_id
                7. session_id
                8. location
                9. user_agent
        - **Dimension tables**:
            1. **users**:
                1. user_id      
                2. first_name
                3. last_name
                4. gender
                5. level        
            2. **songs**:
                1. song_id      
                2. title
                3. artist_id
                4. year
                5. duration
            3. **artists**:
                1. artist_id   
                2. name
                3. location
                4. lattitude
                5. longitude
            4. **time**
                1. start_time  
                2. hour
                3. day
                4. week
                5. month
                6. year
                7. weekday
        
The **songplays** table will help us to accomplish our goal, because it obtains all keys from all other dimension tables,  
so we can easy join with one of the dimension table to answer questions.  
    
## Dataset used:
Data sets used are resides in AWS S3, in a directory of json logs.

