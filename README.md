# Sparkify
Udacity Data Engineering Nanodegree

This repository is part of the Udacity Data Engineering Nanodegree and focuses on data modeling of a relational database.

## Data

The data is taken from the Million Songs dataset and a event generator.

## Run

To run the script you need to put the data in the folder `data` in the root directory.
Install all dependencies via `pip install -r requirements.txt`.
Start the database via `docker-compose up -d`. Then login and create the necessary user and database:
`docker exec -it sparkify_postgres bash` and `psql -U postgres` then `CREATE ROLE student WITH PASSWORD 'student' LOGIN SUPERUSER`.