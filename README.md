# 🚚 E-commerce — Data Pipeline ETL

<p align="center">
  <img src="https://img.shields.io/badge/Status-In%20Development-yellow" alt="Project Status"/>
</p>

<p align="center">
  <b>From Raw CSV Files to Structured Data for Supply Chain Analytics</b>
</p>

This project focuses on building an **Automated ETL (Extract, Transform, Load) pipeline** to process various E-commerce-related CSV files. The pipeline is designed to minimize manual data processing, integrate data from multiple sources, and store the transformed output into a PostgreSQL database, making it readily accessible for data analysis and visualization. By leveraging Pandas, Apache Airflow, PostgreSQL, and Docker, this project explores the implementation of a structured, scheduled ETL process that operates within a secure, contained environment.

## 🎯 Project Objectives

The primary objective of this project is to build a more automated and organized data processing pipeline to support E-Commerce analysis.

- **Automated ETL:** Automates the process of reading and handling multiple CSV files from a source folder.
- **Data Transformation:** Performs data cleaning, transformation, and structural adjustments using pandas.
- **Data Integration:** Integrates relevant data from various sources into a more structured dataset.
- **Centralized Storage:** Stores transformed data into a PostgreSQL database for analytical purposes.
- **Workflow Orchestration:** Manages the sequencing, scheduling, and execution of the pipeline using Apache Airflow.
- **Containerized Environment:** Utilizes Docker to simplify service management and ensure consistency across execution environments.
- **E-Commerce Analytics:** Prepares processed data for use in subsequent analysis and visualization stages.

## 🛠️ Tech Stack

| Technology | Role | Description |
|---|---|---|
| <img src="https://cdn.simpleicons.org/postgresql/4169E1" width="20" alt=""/> **PostgreSQL** | Database | Menyimpan data hasil ETL agar dapat diakses untuk analisis dan kebutuhan downstream. |
| <img src="https://cdn.simpleicons.org/apacheairflow/017CEE" width="20" alt=""/> **Apache Airflow** | Orchestration | Mengatur dependensi, penjadwalan, dan monitoring task dalam pipeline ETL. |
| <img src="https://cdn.simpleicons.org/docker/2496ED" width="20" alt=""/> **Docker** | Containerization | Menjalankan layanan dan dependensi dalam lingkungan container yang terisolasi. |


### 🔄 ETL Workflow

#### 1. Extract — Data Ingestion

The pipeline reads CSV files located in the source folder. This approach allows multiple datasets to be processed through the same workflow, eliminating the need for manual, individual processing.

#### 2. Transform — Data Processing

Pandas is used to perform data transformations as needed, such as:

- Examining column structures and data types.
- Handling missing values ​​and data inconsistencies.
- Cleaning or adjusting data formats.
- Merging datasets that share relationships.
- Preparing data structures for storage and analysis.

The actual transformations depend on the characteristics of the dataset and the business rules applied to the pipeline.

#### 3. Load — Data Storage

The transformed data is loaded into PostgreSQL, where it is stored in a more organized structure, allowing for reuse without the need to repeat the entire processing workflow from the source files.

#### 4. Orchestration — Automated Execution

Apache Airflow is used to define and manage the ETL workflow via a DAG (*Directed Acyclic Graph*). Through this orchestration, processes can be configured to run on a schedule and monitored via the Airflow interface.

#### 5. Supporting Services — Docker

Redis is included as a supporting service based on the project's configuration requirements. Docker is used to run the pipeline components and their dependencies within a consistent environment.

#### 6. Analytics — Downstream Usage

Data stored in PostgreSQL can be accessed by analytical or visualization tools—such as Power BI and Tableau—to facilitate data exploration and the creation of e-commerce dashboards.

> **Note:** Dashboard integration is a downstream phase that can be developed once the pipeline and data structure are ready.

<!-- ## 📂 Project Structure

Contoh struktur repositori yang dapat disesuaikan dengan implementasi aktual:

<!-- ```text
ETLogistic/
│
├── data/
│   ├── raw/                 # File CSV sumber
│   └── processed/           # Output hasil transformasi (opsional)
│
├── dags/
│   └── logistic_etl_dag.py  # DAG untuk orkestrasi ETL
│
├── Kode/
│   ├── Silver_layer.py           # Proses transformasi data menggunakan pandas
│   ├── Gold_layer.py         # Agregasi data sesuai kebutuhan bisnis
│   └── load_db.py              # Proses pemuatan ke PostgreSQL
│
├── notebooks/               # Eksplorasi dan pengujian (opsional)
│
├── docker-compose.yml       # Konfigurasi layanan Docker
├── Dockerfile               # Konfigurasi image (jika digunakan)
├── requirements.txt         # Dependensi Python
├── .env            # Template environment variables
├── .gitignore
└── README.md
``` -->

*Struktur di atas merupakan contoh organisasi proyek. Nama folder dan file dapat disesuaikan dengan struktur repositori yang sebenarnya.* -->

## ⚙️ Prerequisites

Ensure that your device and development environment meet the following requirements:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) or Docker Engine.
- [Python](https://www.python.org/) (a version compatible with the project's dependencies).
- [Git](https://git-scm.com/) for version control.
- WSL 2, if running Docker and ETL processes via a Linux environment on Windows.
- Source CSV files, placed in the configured directory.
- PostgreSQL connection configuration and any other supporting services required by Docker Compose.

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone <repository-url>
cd project-logistic-etl
```

Replace `<repository-url>` with the GitHub repository URL for the project.

### 2. Configure Environment Variables

If the project uses an `.env` file, create one based on the provided template.

```bash
cp .env.example .env
```

Adjust the database connection settings and other service configurations to match your local environment. Do not upload actual credentials to a public repository.

### 3. Prepare Source Data

Place the CSV files in the source directory as required by the pipeline configuration.

```text
Kode/
└── Dataset/
    └── Ecommerce/
        └── train
              ├── df_Customers.csv
              ├── df_OrderItems.csv
              ├── df_Orders.csv
              ├── df_Payments.csv
              └── df_Products.csv
```

The filenames above are for illustration purposes only. Use the actual datasets provided for the project.

### 4. Start Services with Docker

If a `docker-compose.yml` file has been configured to run all necessary services:

```bash
docker compose up -d --build
```

Check the status of the containers:

```bash
docker compose ps
```

Check the service logs if any issues arise:

```bash
docker compose logs
```
```

### 5. Run the ETL Pipeline

Once all services are ready, run the pipeline via the Airflow DAG according to the project configuration. Verify the database connections and the locations of the source files.

If Airflow is using the standard port mapping, its interface can be accessed at:

```text
http://localhost:8080
```

Activate or run the ETL DAG according to the settings available in the Airflow environment.

### 6. Verify the Output

Check the ETL result tables in PostgreSQL to ensure that the data has been successfully loaded and is ready for further analysis.

## 📊 Expected Outcome

The desired outcomes for this project include:

- Processing multiple CSV files through a single, structured ETL pipeline.
- Reducing manual effort in the data ingestion and transformation processes.
- Storing transformed data centrally in a PostgreSQL database.
- Establishing a workflow that can be scheduled and monitored via Apache Airflow.
- Ensuring a more consistent execution environment through the use of Docker.
- Preparing datasets that are ready for further development to support e-commerce analysis and visualization.

## 🔮 Future Improvements

Potential future enhancements include:

- **Data Quality Checks:** Implementing schema validation, handling null values, identifying duplicates, and ensuring data integrity.
- **Incremental ETL:** Processing only new or modified data.
- **Pipeline Monitoring:** Adding logging, failure notifications, and task duration monitoring.
- **Data Modeling:** Developing analytical schemas, such as a star schema, if required for reporting purposes.
- **BI Integration:** Connecting PostgreSQL to tools like Power BI or Tableau to build dashboards.
- **Performance Optimization:** Adjusting partitioning, data sizing, and Spark configurations based on processing requirements..