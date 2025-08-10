# Weather Data Engineering Pipeline

## 📌 Project Overview
This project implements a **real-time data engineering pipeline** that collects weather data from an API, processes it, stores it in **Snowflake**, and performs **real-time analytics** using **Apache Spark**. The pipeline is automated with a **daily Bash script** for consistent execution.

---

## ⚙️ Pipeline Components

### 1. **Weather Data API Fetcher**
- **Description:** Python script that performs API calls to gather weather data for requested cities.
- **Output:** Saves the retrieved data into a **SnowFlake database**.
- **Purpose:** Serves as a **Kafka producer** to publish weather data for further processing.

### 2. **Kafka Producer & ETL**
- **ETL Steps:**
  - **Extract:** Fetch weather data from the API results.
  - **Transform:** Clean, normalize, and structure the data.
  - **Load:** Publish transformed data to a Kafka topic.
- **Tech Stack:** Python, Apache Kafka.
- **Output:** Data is sent to the Kafka broker for downstream consumers.

### 3. **Kafka Consumer → Snowflake**
- **Description:** Kafka consumer that subscribes to the weather data topic.
- **Functionality:**
  - Performs data ingestion into **Snowflake Data Warehouse**.
  - Uses **embedded SQL queries** for data insertion and table management.
  - Reads Snowflake connection details from a **Snowflake config file**.

### 4. **Real-Time Analytics with Spark**
- **Description:** Apache Spark job that consumes Kafka stream data for **real-time analysis**.
- **Purpose:** Enables quick insights, trend detection, and live monitoring.

### 5. **Automation (Bash Script)**
- **Description:** Bash script that runs the entire pipeline **once per day**.
- **Tasks:**
  1. Fetch weather data.
  2. Start Kafka producer.
  3. Trigger Kafka consumer to Snowflake.
  4. Run Spark analytics.
- **Execution:** Can be scheduled via **cron job** or other scheduling tools.

---

## 🛠️ Tech Stack
- **Programming Languages:** Python, Bash
- **Data Streaming:** Apache Kafka
- **Data Processing:** Apache Spark
- **Data Warehouse:** Snowflake
- **Scheduling:** Cron, Bash
- **Data Format:** CSV, JSON

---

## 📂 Project Structure

<img width="1660" height="400" alt="(untitled)" src="https://github.com/user-attachments/assets/88a2a8b8-34e5-404a-bc22-eac72462c521" />


