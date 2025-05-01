flowchart LR
    %% ────────────────────────────
    %% Stage I — Data Collection
    %% ────────────────────────────
    subgraph "Stage I — Data Collection"
        Kaggle["Kaggle CLI<br/>(job-descriptions.csv)"]
        PostgreSQL["PostgreSQL"]
        Sqoop["Sqoop Import"]
        HDFS1["HDFS<br/>Avro (+ schema)"]

        Kaggle --> PostgreSQL
        PostgreSQL --> Sqoop
        Sqoop --> HDFS1
    end

    %% ────────────────────────────
    %% Stage II — Data Warehouse & EDA
    %% ────────────────────────────
    subgraph "Stage II — Data Warehouse & EDA"
        Hive["Hive External Tables<br/>(partitioned & bucketed)"]
        SparkSQL["Spark SQL<br/>(6 analyses)"]

        Hive --> SparkSQL
    end

    %% ────────────────────────────
    %% Stage III — Predictive Analytics
    %% ────────────────────────────
    subgraph "Stage III — Predictive Analytics"
        SparkML["Spark ML Pipeline"]
        LR["Linear Regression"]
        GBT["Gradient-Boosted Trees"]

        SparkML --> LR
        SparkML --> GBT
    end

    %% ────────────────────────────
    %% Stage IV — Presentation & Delivery
    %% ────────────────────────────
    subgraph "Stage IV — Presentation & Delivery"
        HiveExt["Hive Externals<br/>(metrics & predictions)"]
        Superset["Apache Superset<br/>Dashboards"]

        HiveExt --> Superset
    end

    %% ────────────────────────────
    %% Cross-stage flow
    %% ────────────────────────────
    HDFS1 --> Hive
    SparkSQL --> SparkML
    LR --> HiveExt
    GBT --> HiveExt
