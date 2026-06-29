# Git Event Data Pipeline

## Overview

This project explores how a full data pipeline is built end-to-end, from raw ingestion to transformation and visualization. It was created as a hands-on learning experience to understand how data flows through real-world systems, how different tools integrate, and how batch pipelines are designed under practical constraints. The final output is a set of analytical datasets and dashboards built from large-scale GitHub event data.

## Architecture

The pipeline follows a production-inspired batch flow:



## Tech Stack

* GH Archive (Data Source)
* Airbyte (Data Ingestion)
* Amazon S3 (Storage)
* Databricks (Processing & Dashboards)
* Apache Spark (Data Processing)
* Tailscale (Local server exposure)
* Python (Preprocessing, Transformation)
* SQL (Tranformation, Analytics Queries)

## Data Source

Initially, the GitHub REST API was evaluated, but limitations on data volume made it unsuitable for large-scale ingestion.

The project was switched to GH Archive, which provides high-volume GitHub event data in compressed JSONL format. This enabled more realistic data engineering workflows while staying within hardware constraints.

Local testing and validation were performed using tools like Postman and direct data pulls to ensure reliability before integration.

## Data Ingestion

A batch ingestion approach was chosen over streaming due to hardware limitations and project scope.

Airbyte was used as the ingestion tool because of its open-source ecosystem and wide range of connectors. However, integrating GH Archive data introduced challenges due to its gzip-compressed JSONL format, which required a custom preprocessing step before ingestion. 

A local processing layer was built to extract and restructure the data, and a local server was then exposed using Tailscale Funnels to make the prepared data accessible to Airbyte despite Docker filesystem limitations.

## Data Storage

Amazon S3 was used as the central storage layer for the pipeline. An S3 bucket was created and configured with the necessary permissions, including IAM roles and policies that allowed Airbyte to write data securely. 

Once credentials were configured, Airbyte was able to reliably push ingested batch data into S3, completing the ingestion and storage layer of the pipeline.

Here is the AWS Policy attached to the Airbyte user: [AWS Airbyte Policy](https://github.com/SalihAlwassiti/Git-Event-Data-Pipeline/blob/main/aws_airbyte_policy.json)

## Data Processing

Databricks and Apache Spark were used to process and transform the data at scale.

A Medallion Architecture was implemented to structure the transformations across three layers:

* **The Bronze layer** stored ingested data from Airbyte. During ingestion, JSONL was converted to CSV, and the data was then stored without further transformations.

* **The Silver layer** processed and parsed the JSON event fields, cleaned the data, and extracted only the relevant attributes needed for analysis.

* **The Gold layer** consisted of multiple aggregated tables designed for analytics, including event summaries, contributor activity metrics, repository activity metrics, time-based distributions, and breakdowns of bot versus organization activity.


All transformations were designed to support incremental updates to ensure scalability and efficiency.

The bronze to silver to gold transformation codes: [Bronze To Silver](https://github.com/SalihAlwassiti/Git-Event-Data-Pipeline/blob/main/bronze_to_silver.py), [Silver To Gold](https://github.com/SalihAlwassiti/Git-Event-Data-Pipeline/blob/main/silver_to_gold.sql)

Note: While I designed the core pipeline logic and data transformations, I utilized the Databricks AI Assistant to refine the Spark execution plans and implement incremental updates.

## Visualization

Tableau was considered, but instead Databricks native dashboards were used for simplicity and integration.

### Built dashboards include:

* Total number of events
* Total contributors
* Total repositories
* GitHub event type distribution
* Heatmap of activity (day of week vs hour of day)
* Top 10 most active contributors
* Top 10 most active repositories
* Percentage of bot vs organization activity
* Event contribution breakdown by bots and organizations

<img width="6000" height="8946" alt="GitHub Event Statistics-20260629_11-15_page-0001" src="https://github.com/user-attachments/assets/544ad647-b4d7-40d9-a78c-82c8204a9142" />

## Future Improvements

**Local preprocessing**: Replace manual preprocessing with a more automated and scalable approach

**Data orchestration**: Introduce workflow orchestration tools to schedule and manage pipeline jobs

**Data validation**: Add validation layers to ensure data quality, consistency, and schema enforcement
