# RER Train Schedule Visualization

This project fetches and visualizes RER train schedules for the Paris region using Scala, Apache Spark, and Python.

## Features

- Fetches RER train data from the SNCF API
- Processes and combines data using Apache Spark
- Visualizes train movements on an interactive map

## Requirements

- Java 8
- Scala 2.12
- Apache Spark 3.x
- Python 3.x
- Folium (Python library for map visualization)

## Project Structure

- `core/src/main/scala/`: Scala source files
  - `MainClass.scala`: Main entry point for data fetching and processing
  - `SparkSessionTrait.scala`: Trait for creating Spark sessions
  - `scheduler/TaskScheduler.scala`: Scheduler for periodic tasks
  - `processor/DataProcessor.scala`: Data processing functions
  - `downloader/JsonDownloader.scala`: JSON data downloader
- `core/notebook_test.ipynb`: Jupyter notebook for data visualization

## Setup

1. Ensure Java 8 is installed on your system.
2. Install Scala and Apache Spark.
3. Install Python and required libraries:

```

pip install folium geopandas pandas

```

4. Clone this repository:

```

git clone https://github.com/yourusername/rer-train-schedule-visualization.git

```

## Usage

1. Run the Scala application to fetch and process data:

```

sbt run

```

2. Open `core/notebook_test.ipynb` in Jupyter Notebook or JupyterLab to visualize the data.

## Note on Java Version

This project is designed to work with Java 8. Make sure you have Java 8 installed and set as your default Java version before running the application.
