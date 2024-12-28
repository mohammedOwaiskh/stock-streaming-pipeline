# Stock Market Data Pipeline

## Overview
The **Stock Market Data Pipeline** is a real-time data engineering project designed to ingest, process, store, and analyze stock market data from various sources. The pipeline leverages modern tools and technologies to handle high-frequency data streams, perform real-time computations, and provide actionable insights.

## Features
- **Real-Time Data Ingestion**: Fetch live stock market data using APIs and stream it to Kafka.
- **Data Processing**: Perform real-time calculations like moving averages, anomaly detection, and trend analysis using PySpark.
- **Data Storage**: Store raw and processed data in AWS S3 and a data warehouse for historical analysis.
- **Analytics and Visualization**: Generate insights and visualize trends using dashboards.
- **Alerts**: Set up notifications for significant market movements.

### Components
1. **Data Ingestion**:
   - APIs: Finnhub
   - Kafka for streaming data

2. **Data Processing**:
   - PySpark Structured Streaming
   - Kafka Streams for real-time processing

3. **Data Storage**:
   - AWS S3 for raw and historical data
   - AWS S3 for processed Data

4. **Orchestration**:
   - Apache Airflow for managing ETL workflows

## Getting Started

### Prerequisites
- Python 3.8+
- Kafka
- PySpark
- AWS account with S3 access
- API key for [Finnhub][finnhub.io] account

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/mohammedOwaiskh/stock-streaming-pipeline.git
   cd stock-streaming-pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   - `KAFKA_BROKER_URL`
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `STOCK_API_KEY`

### Running the Pipeline
1. Start Kafka:
   ```bash
   ./bin/kafka-server-start.sh config/server.properties
   ```

2. Run the data producer:
   ```bash
   python producer.py
   ```

3. Start the data processor:
   ```bash
   spark-submit processor.py
   ```

4. Visualize data using your chosen dashboard tool.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact
For questions or feedback, reach out to [mohdowaiskh@gmail.com].

