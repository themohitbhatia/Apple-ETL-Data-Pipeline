## 📌 Project Overview
This data engineering project focuses on analyzing Apple customer purchase patterns to derive actionable marketing insights. By leveraging the distributed computing power of PySpark within Databricks, the system processes large-scale sales data to identify specific purchasing sequences and customer loyalty segments.

The project is built using a modular, Object-Oriented Programming (OOP) approach, ensuring that data ingestion, transformation, and analysis are decoupled and maintainable.

## 🛠️ Problem Statements
The project provides solutions to two primary business questions:

1. Sequence Analysis (Upsell Pattern): Identify customers who purchased AirPods immediately following an iPhone purchase. This helps in understanding the effectiveness of ecosystem cross-selling.

2. Exclusivity Analysis (Segment Discovery): Identify customers who have exclusively bought iPhones and AirPods and no other products from the catalog.

## 🏗️ Architecture & Design
The project follows a modular class-based structure:
The project follows a Modular ETL Architecture decoupled by the Factory Design Pattern. This design ensures that the pipeline is highly extensible, allowing for different data sources or sinks to be added without modifying the core business logic.

### 🧩 Core Design Patterns
- Factory Pattern: Implemented in the factories/ directory. This pattern abstracts the instantiation of Reader and Loader objects, allowing the pipeline to support multiple file formats (Delta, Parquet, CSV) or storage systems dynamically.

- Separation of Concerns: Each stage of the ETL process is isolated into dedicated modules. This modularity facilitates unit testing and simplifies the debugging of specific transformation steps.

### 📦 Component Breakdown
1. Factories (factories/)
    1. Reader_Factory.py: A centralized creator class that returns specific reader objects based on the input configuration.

    2. Loader_Factory.py: Manages the creation of writer objects, handling logic for different output modes such as "Overwrite," "Append," or "Upsert" (Merge) into Delta tables.

2. ETL Modules (modules/)
    1. Extractor.py: Responsible for the "Extract" phase. It interacts with the Reader_Factory to fetch raw data into a Spark DataFrame and performs initial schema enforcement.

    2. Transformer.py: The heart of the application. It contains the PySpark logic for solving the business problems statements mentioned above.

    3. Loader.py: Manages the "Load" phase. It utilizes the Loader_Factory to ensure data is written to the correct destination with appropriate partitioning and indexing.

3. Orchestrator (Apple_ETL.ipynb)
This notebook serves as the Controller. It handles environment configuration, initializes the ETL components, and manages the end-to-end execution flow.


## 📂 Project Structure
    APPLE_ETL_DATA_PIPELINE/
    ├── factories/
    │   ├── Loader_Factory.py     # Logic to instantiate appropriate data writers
    │   └── Reader_Factory.py     # Logic to instantiate appropriate data readers
    ├── modules/
    │   ├── Extractor.py          # Data ingestion logic (Extract)
    │   ├── Transformer.py        # Core Business Logic (Transform)
    │   └── Loader.py             # Data sink management (Load)
    └── Apple_ETL.ipynb           # Databricks Orchestration Notebook


## 🚀 Getting Started
1. Import to Databricks: Clone this repo into your Databricks Workspace using Repos.

2. Data Setup: Ensure your sales data is available in a Spark-accessible location (DBFS/S3/ADLS).

3. Run Pipeline: Open notebooks/main_orchestrator and run all cells.