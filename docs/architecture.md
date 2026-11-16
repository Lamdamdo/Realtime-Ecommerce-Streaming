# System Architecture

```mermaid
flowchart LR
    A["Python Producer<br/>Synthetic Orders"] --> B["Confluent Cloud<br/>Kafka Topic: Orders"]
    B --> C["Databricks<br/>Spark Structured Streaming"]

    C --> D["Bronze Layer<br/>Raw Events"]
    D --> E["Silver Layer<br/>Cleaned Events"]
    E --> F["Gold Layer<br/>Business Aggregations"]

    F --> G["Power BI<br/>2-Page Dashboard"]

    C -.-> H["Checkpoint Storage<br/>Unity Catalog Volume"]
