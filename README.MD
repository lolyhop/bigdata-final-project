# Loan Default Prediction Using a Distributed Big Data Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Course-Big%20Data%20IU-blue" />
  <img src="https://img.shields.io/badge/Task-Binary%20Classification-purple" />
  <img src="https://img.shields.io/badge/Dataset-2.26M%20Loans-orange" />
  <img src="https://img.shields.io/badge/Pipeline-End--to--End-green" />
  <img src="https://img.shields.io/badge/Pylint-9.97%2F10-brightgreen" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/PostgreSQL-Citus-336791" />
  <img src="https://img.shields.io/badge/Sqoop-HDFS-yellow" />
  <img src="https://img.shields.io/badge/Hive-Tez-purple" />
  <img src="https://img.shields.io/badge/Spark-MLlib-orange" />
  <img src="https://img.shields.io/badge/Superset-Dashboard-20A6C9" />
</p>

## Navigation

- [Business Value](#business-value)
- [Project Goal](#project-goal)
- [Dataset](#dataset)
- [Repository Structure](#repository-structure)
- [Pipeline Overview](#pipeline-overview)
- [Pipeline Stages](#pipeline-stages)
- [Stage I — Data Collection and Ingestion](#stage-i--data-collection-and-ingestion)
- [Stage II — Hive Storage and EDA](#stage-ii--hive-storage-and-eda)
- [Stage III — Predictive Data Analytics](#stage-iii--predictive-data-analytics)
- [Stage IV — Dashboard Preparation](#stage-iv--dashboard-preparation)
- [Running the Full Pipeline](#running-the-full-pipeline)
- [Running Individual Stages](#running-individual-stages)
- [Key Outputs](#key-outputs)
- [Code Quality](#code-quality)

End-to-end distributed pipeline for loan default prediction using PostgreSQL, Sqoop, HDFS, Hive, Spark ML on YARN, and Apache Superset.

This repository contains the final project for the Big Data course at Innopolis University. The project uses the Lending Club accepted loans dataset to build a reproducible big data pipeline for exploratory analytics, predictive modeling, and dashboard visualization.

## Business Value

Loan default prediction is important for lending platforms because it helps estimate credit risk before issuing a loan. A reliable risk model can support safer lending decisions, reduce potential losses from charged-off loans, and make underwriting more consistent.

This project combines predictive modeling with portfolio-level analytics. The machine learning part identifies loans with higher default risk, while the dashboard helps stakeholders explore default patterns across loan grades, purposes, FICO scores, debt-to-income ratios, and other borrower characteristics.

## Project Goal

The goal is to predict whether an accepted loan will end as:

- `Fully Paid`
- `Charged Off`

This is formulated as a binary classification task using only loan origination-time features.

## Dataset

Dataset: Lending Club accepted loans dataset.

Main characteristics:

- 2,260,701 loan records
- 151 raw attributes
- approximately 1.68 GiB
- contains datetime fields such as `issue_d` and `earliest_cr_line`
- target column: `loan_status`

The raw dataset is not stored in GitHub. It is downloaded during preprocessing and stored locally or on the cluster.

## Repository Structure

```text
data/       train/test JSON artifacts and local dataset files
models/     trained Spark ML models exported from HDFS
notebooks/  experimental notebooks, not required for pipeline execution
output/     CSV outputs, EDA results, dashboard tables, screenshots
scripts/    Python and shell scripts for all pipeline stages
sql/        SQL and HiveQL scripts
logs/       execution logs, if generated
```

## Pipeline Overview

Raw CSV
  ↓
Preprocessing
  ↓
PostgreSQL
  ↓ Sqoop
HDFS Parquet + Snappy
  ↓
Hive tables
  ↓
HiveQL EDA
  ↓
Spark ML on YARN
  ↓
Superset dashboard outputs

The pipeline is designed to be reproducible. Each stage is implemented as a script and can be re-run independently. Intermediate outputs are overwritten or recreated when needed.

## Pipeline Stages

The project is organized into four reproducible stages.

### Stage I — Data Collection and Ingestion

This stage downloads the raw Lending Club dataset, filters and normalizes the selected columns, loads the cleaned data into PostgreSQL, and imports the table into HDFS using Sqoop. The ingested data are stored in Parquet format with Snappy compression.

**Main steps:**

- download the raw dataset;
- select loan origination-time columns;
- normalize dates, percentages, and loan term values;
- load cleaned data into PostgreSQL table loans;
- import PostgreSQL data to HDFS using Sqoop;
- store HDFS data as Parquet with Snappy compression.

**Main scripts:**

```text
scripts/preprocess.sh
scripts/stage1.sh
scripts/download_from_yandex_disk.py
scripts/filter_dataset_for_pg.py
scripts/load_postgres.py
```

### Stage II — Hive Storage and EDA

This stage registers the HDFS Parquet data in Hive, fixes date columns, and creates the final analytical Hive table. The table is partitioned by grade, bucketed by id, and stored as Snappy-compressed Parquet. The stage also runs HiveQL-based EDA queries and exports the results to `output/`.

**Main steps:**

- create Hive database team25_projectdb;
- create staging Hive table over Sqoop Parquet files;
- convert timestamp fields to date fields;
- create final external Hive table;
- partition the table by grade;
- bucket the table by id into 8 buckets;
- run business EDA queries;
- run data quality EDA queries;
- export EDA results to CSV.

**Main scripts:**

```text
scripts/stage2.sh
scripts/load_hive.py
scripts/eda/q1.py ... scripts/eda/q6.py
scripts/run_hql_export.py
```

**Main SQL/HQL folders:**

```text
sql/eda/
sql/data_quality/
```

**Business EDA outputs:**

```text
output/q1.csv  
output/q2.csv  
output/q3.csv  
output/q4.csv  
output/q5.csv  
output/q6.csv  
```

**Data quality EDA outputs:**

```text
output/dq_null_profile.csv  
output/dq_loan_status_distribution.csv  
output/dq_binary_target_distribution.csv  
output/dq_categorical_value_counts.csv  
output/dq_numeric_summary.csv  
output/dq_numeric_distributions.csv  
```

**EDA insights include:**

- default rate by loan grade;
- default rate by loan purpose;
- default rate over time;
- default rate by debt-to-income ratio bucket;
- interaction between loan grade and home ownership;
- relationship between FICO score and default risk.

### Stage III — Predictive Data Analytics

This stage prepares the binary classification dataset and trains Spark ML models on YARN. It performs feature engineering, train/test splitting, missing value imputation, categorical encoding, train balancing, baseline training, hyperparameter tuning with grid search and 3-fold cross-validation, and model evaluation.

**Main steps:**

- keep only finalized target classes: Fully Paid and Charged Off;
- convert the target into a binary label;
- engineer datetime features from `issue_d` and `earliest_cr_line`;
- split the dataset into 70% train and 30% test;
- impute missing values using train-only mode;
- encode categorical features with `StringIndexer` and `OneHotEncoder`;
- assemble features with `VectorAssembler`;
- balance only the training split;
- keep the test split unchanged;
- train baseline models;
- tune models with grid search and 3-fold cross-validation;
- export metrics, predictions, and trained models.

**Models:**

- Random Forest Classifier;
- Linear Support Vector Classifier;
- Naive Bayes Classifier.

**Main scripts:**

```text
scripts/stage3.sh  
scripts/prepare_raw_ml_dataset.py  
scripts/build_train_test_datasets.py  
scripts/train_baseline_models.py  
scripts/train_tuned_models.py  
scripts/ml_utils.py  
```

**Main Stage III outputs:**

```text
output/baseline_evaluation.csv  
output/evaluation.csv  
output/prediction_distribution.csv  
output/split_summary.csv  
output/feature_info.csv  
output/label_distribution.csv  
output/dashboard/  
models/model1/  
models/model2/  
models/model3/  
data/train.json  
data/test.json  
```

The trained models are first saved to HDFS and then exported to the local models/ directory for repository submission.

### Stage IV — Dashboard Preparation

This stage registers Stage III CSV outputs as external Hive tables so that Apache Superset can use them as datasets. The dashboard presents data characteristics, EDA insights, model metrics, and prediction results.

**Main steps:**

- read Stage III CSV output directories from HDFS;
- create external Hive tables over the CSV outputs;
- expose ML and dataset summary tables to Superset;
- build dashboard charts from Hive-backed Superset datasets.

**Main scripts:**

scripts/stage4.sh  
scripts/load_ml_hive_tables.py  

**Main Hive tables:**

- ml_split_summary
- ml_feature_info
- ml_label_distribution
- ml_baseline_evaluation
- ml_evaluation
- ml_prediction_distribution

**Dashboard sections:**

- data description
- data quality and dataset characteristics
- EDA business insights
- ML model performance
- prediction results

## Running the Full Pipeline

The full pipeline is executed through:

```bash
bash main.sh
```

The project template requires `main.sh` to be the main entrypoint for assessment. The script runs all stages and stores final results in the repository output folders.

Before running, create and configure `.env` with PostgreSQL, Hive, HDFS, and ML paths.

Example:

```bash
set -a
source .env
set +a
bash main.sh
```

Spark ML jobs must run on YARN and not in local mode.

## Running Individual Stages

Each stage can also be executed independently:

```bash
bash scripts/preprocess.sh
bash scripts/stage1.sh
bash scripts/stage2.sh
bash scripts/stage3.sh
bash scripts/stage4.sh
```

## Key Outputs

The most important outputs for grading and dashboarding are:

```text
- output/q1.csv ... output/q6.csv
- output/dq_*.csv
- output/baseline_evaluation.csv
- output/evaluation.csv
- output/prediction_distribution.csv
- output/split_summary.csv
- output/feature_info.csv
- output/label_distribution.csv
- output/dashboard/
- models/model1/
- models/model2/
- models/model3/
- data/train.json
- data/test.json
```

## Code Quality

Pylint can be run with:

```bash
PYTHONPATH="$PWD/scripts" python3 -m pylint scripts
```

Current code quality check result:

```
10/10
```
