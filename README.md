# API Event ETL Workflow

This project implements an ETL (Extract, Transform, Load) process for event data from a public API using Apache Airflow on Astronomer. The ETL workflow fetches event data, transforms it, and loads it into a PostgreSQL database.


## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)


## Overview

The ETL process consists of the following steps:

1. **Extract**: Fetch event data from an API.
2. **Transform**: Clean and structure the data for loading into PostgreSQL.
3. **Load**: Insert the cleaned data into a PostgreSQL database.



### Prerequisites

- [Astronomer CLI](https://www.astronomer.io/docs/cli)
- [PostgreSQL](https://www.postgresql.org/download/)
- Python 3.9 or higher
- Docker installed on your machine 

## Setup Instructions
1. **Install the Astronomer CLI**:
   Follow the instructions on the [Astronomer CLI documentation](https://www.astronomer.io/docs/cli/install-cli) to install the CLI on your machine.


2. **Set up a virtual environment**:
   Create a new directory for your project and navigate into it. Then set up a virtual environment:
   ```bash
   mkdir etl_pipeline
   cd etl_pipeline
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`

3. **Initialize the Astronomer project**: Run the following command to create a new Astronomer project:
```bash
astro dev init
```
4. Create a docker-compose.yml file: Create a docker-compose.yml file in your project directory to set up PostgreSQL:
```bash
version: "1"
services:
  postgres:
    image: postgres:13
    container_name: postgres_db
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
volumes:
  postgres_data:
```
5. **Create your DAG configuration**: Navigate to the dags directory in your project and create your DAG configuration file (e.g., api_event_etl_dag.py). You can follow Airflow's documentation for guidance on creating DAGs.

## Usage 
1. Start the Astronomer development environment:
```bash 
astro dev start
``` 
2. access Access the Airflow UI at http://localhost:8080 and find the DAG named api_event_etl_dag 
3. Trigger the DAG manually or set a schedule for automatic runs.
4. Monitor the execution: Once triggered, you can monitor the execution of the DAG in the Airflow UI, check the status of each task, and view logs for debugging.
