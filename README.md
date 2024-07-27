# Project Instructions

Using the following tech stack AWS Glue, AWS S3, Python, and Spark
Create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI
data scientists.

# Requirements

To simulate the data coming from the various sources, you will need to create your own S3 directories for the following
zones

- customer_landing
- step_trainer_landing
- accelerometer_landing

1. Do some exploratory work and create two Glue tables for the two landing zones. 
2. Query those tables using Athena

## Problem 1

The Data Science team has done some preliminary data analysis and determined that the Accelerometer Records each match
one of the Customer Records. They would like you to create 2 AWS Glue Jobs that do the following:

1. Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share
   their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.
2. Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from
   customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called
   accelerometer_trusted.
3. You need to verify your Glue job is successful and only contains Customer Records from people who agreed to share
   their data. Query your Glue customer_trusted table with Athena and _**take a screenshot of the data**_. Name the screenshot
   customer_trusted(.png,.jpeg, etc.).

## Problem 2

Data Scientists have discovered a data quality issue with the Customer Data.

1. The serial number should be a **unique identifier** for the STEDI Step Trainer they purchased. However, there was a
   defect in the fulfillment website, and it used the same 30 serial numbers over and over again for millions of
   customers! Most customers have not received their Step Trainers yet, but those who have, are submitting Step Trainer
   data over the IoT network (Landing Zone). The data from the Step Trainer Records has the correct serial numbers.
2. The problem is that because of this serial number bug in the fulfillment data (Landing Zone), we don’t know which
   customer the Step Trainer Records data belongs to.

The Data Science team would like you to write a Glue job that does the following:

1. Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who
   have accelerometer data and have agreed to share their data for research called customers_curated.

## Finally

I created two Glue Studio jobs that do the following tasks:

1. Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that
   contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data
   for research (customers_curated).
2. Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data
   for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called
   machine_learning_curated.

---

# Solution

## Github Project Artefacts

- [x] Setup environment
  - [x] upload s3 content to local sandbox
  - [x] create athena tables to explore the 2 landing zones (see [images](./images) )
- [x] (In order) create scripts to
  - [x] create trusted zones for customer and accelerometer
  - [x] create a curated zone for customer
  - [x] create trusted zone for step_trainer
  - [x] create curated zone for ml

---

