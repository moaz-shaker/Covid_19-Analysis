# COVID Data Visualization Pipeline

This project builds a pipeline workflow for COVID data visualization using HDFS, Hive, Oozie on Cloudera HUE, with final visualization in Power BI.

## Table of Contents

- [Introduction](#introduction)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Project Setup](#project-setup)
- [Data Flow](#data-flow)
- [Usage](#usage)
- [Visualization](#visualization)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Introduction

The purpose of this project is to develop a data pipeline that ingests, processes, and visualizes COVID-19 data using Big Data technologies for efficient processing and analysis. The processed data is then visualized in Power BI for insight-driven decision-making.

## Architecture

1. **Data Ingestion:** COVID data is ingested and stored in HDFS.
2. **Data Processing:** Hive is used to aggregate and analyze the data.
3. **Workflow Automation:** Oozie is used to schedule and orchestrate the pipeline workflow.
4. **Visualization:** Power BI is used to visualize the final processed data.

## Technologies Used

- **HDFS (Hadoop Distributed File System):** Distributed storage system for big data.
- **Hive:** SQL-based engine for querying large datasets in HDFS.
- **Oozie:** Workflow scheduling tool for automating and coordinating tasks.
- **Power BI:** Business analytics tool for interactive data visualization.
- **Cloudera HUE:** Web interface for interacting with Hadoop ecosystem components such as HDFS and Hive.

## Project Setup

### Prerequisites

- Cloudera HUE environment with access to HDFS, Hive, and Oozie.
- Power BI Desktop for creating visualizations.
- Python/Java installed for workflow scripting and management.

### Steps

1. **Data Upload:** Upload the COVID dataset (in CSV format) to HDFS using Cloudera HUE.
   - You can use datasets like the [Johns Hopkins COVID-19 Dataset](https://github.com/CSSEGISandData/COVID-19) as a data source.
   
2. **Create Hive Tables:** Define the schema and create Hive tables to store the uploaded COVID data.
   ```sql
   CREATE TABLE covid_data (
     date STRING,
     country STRING,
     confirmed_cases INT,
     deaths INT,
     recovered INT
   ) ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ',';
