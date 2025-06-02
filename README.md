# 🚀 Real-Time Data Pipeline with Apache NiFi, Kafka, and Spark

This project demonstrates the implementation of a real-time data pipeline using a modern data engineering stack. It showcases real-time ingestion, processing, and storage of streaming data—ideal for analytics, monitoring, or alerting systems.

---

## 🛠 Tech Stack

| Component           | Purpose                                              |
|---------------------|------------------------------------------------------|
| **Web API (Python)**| Simulates real-time data production                  |
| **Apache NiFi**     | Ingests data, performs basic transformation/routing  |
| **Apache Kafka**    | Acts as the message broker for real-time streams     |
| **Apache Spark (Scala)** | Consumes Kafka topics, processes the stream   |
| **Local Storage**   | Stores transformed data (CSV/Parquet)                |

---

## 📌 Project Architecture

graph TD;
    A[Web API (Data Producer)] --> B[Apache NiFi]
    B --> C[Apache Kafka]
    C --> D[Apache Spark (Structured Streaming)]
    D --> E[Local Storage (CSV)]
