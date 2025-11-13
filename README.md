# Aviation_Operations_Analytics_Project

## Problem Statement
Airports and aviation authorities often face challenges in monitoring flight operations in real time and analyzing schedule adherence across multiple data sources.
Discrepancies between scheduled and actual flight operations, coupled with the absence of an integrated data platform, make it difficult to evaluate key metrics such as:

- Schedule adherence percentage

- Average delays in arrivals and departures

- Daily or hourly operational efficiency per airport

This project addresses that problem by building a data engineering pipeline that consolidates flight data from multiple APIs, performs structured transformations, and stores results in a data warehouse (Snowflake) for analytics and dashboarding.

## Project Scope

The project focuses on:

1) Data Ingestion and Integration – Extracting flight operation data from public APIs (AeroDataBox and OpenSky Network).

2) Data Orchestration - Automating ingestion workflows using Apache Airflow.

3) Data Modeling and Transformation – Cleaning and structuring data using dbt, implementing incremental models and KPI logic.

4) Data Storage – Organizing data in Snowflake using raw, staging, and curated layers.

5) KPI Development –
   
     * Scheduled vs. actual arrivals and departures

     * Schedule adherence (%)

     * Average arrival and departure delays (minutes)

6) Analytics-Ready Outputs – Preparing tables and metrics suitable for BI dashboards.

## Data sources
1) AeroDataBox API
   
   Provides flight schedule information including planned arrivals and departures, airline, aircraft type, and timings.

2) OpenSky Network API

   Supplies real-time flight state information including actual departure and arrival signals via ADS-B transponders.

## Tech Stack
* Python – Data ingestion and transformation scripts

* Apache Airflow – Workflow orchestration for scheduling and automation

* dbt (Data Build Tool) – SQL transformations, data modeling, incremental pipelines

* Snowflake – Cloud data warehouse for structured storage

* SQL – Data aggregation and KPI computation

* REST APIs – Data acquisition (AeroDataBox, OpenSky Network)

* Power BI / Any BI Tool – Visualization of flight metrics and schedule adherence