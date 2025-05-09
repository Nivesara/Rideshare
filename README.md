# Rideshare Data Analysis using Apache Spark

**Duration**: Mar 2024 - Apr 2024  
**Institution**: Queen Mary University of London

## Overview
This project analyzes over 69 million rideshare records using PySpark. It integrates distributed data processing, geospatial analysis, and performance optimization techniques.

## Key Features
- Engineered PySpark pipelines to join large datasets (`rideshare_data.csv`, `taxi_zone_lookup.csv`)
- Converted UNIX timestamps to human-readable format
- Performed broadcast joins and aggregations (`groupBy`) to compute:
  - Monthly trip counts
  - Driver earnings
  - Rideshare platform profits
- Visualized trip patterns and revenue trends using `matplotlib`
- Integrated with AWS S3 and Hadoop file system for scalable data processing
