**PAGEVIEW COUNT BASED ON DOMAINS DATA WAREHOUSE**

**INTRODUCTION** \
The aim of this project is to develop a data warehouse for analytical purposes, specifically targeting skills at the fresher level. The project focuses on improving batch data collection techniques and the application of ETL/ELT processes using Python and Apache Spark.\
Project Idea:\
•	Data Source: Pageview count data collected from Wikimedia.\
•	Data Structure: The dataset spans multiple years, months, and days, with data collected at 1-hour intervals. Files are stored as compressed .gz archives.\
•	Scope: This project extracts data on pageviews for specific domains and page names, focusing on the following companies: "Google", "Facebook", "Amazon", "Microsoft", "Apple", and "Walmart".\
•	Objective: The processed data is transformed and loaded into a data warehouse (OLAP) for analysis and insights.


**NOTE**\
The final data in the data warehouse may not exactly match Wikimedia's source data. This project is for educational and practice purposes only.

**PROJECT STRUCTURE** 
1.	Source Folder: Contains raw .gz files.
2.	Stage Folder: Holds intermediary files, including Parquet files for processed data.
3.	Data Warehouse: The final OLAP storage for the processed data.
4.	Extract and Visualization: Tableau is used for visualizing key statistics and insights.

**DEPENDENCIES AND TOOLS**\
•	Python: The core programming language for data extraction, transformation, and loading (ETL).\
•	Apache Spark: Used for large-scale parallel data processing and loading data into the data warehouse.\
•	Navicat Premium: A user-friendly tool for database management and development.\
•	Tableau: For creating interactive visualizations and insights.

**SKILLS AND CHALLENGES ADDRESSED**

Challenge 1: Handling Large .gz Files
•	Problem: The .gz files are large, requiring significant storage space.
•	Solution:
o	Download files in batches.
o	Process and extract data for each day (24 files per day).
o	Transform the data into Parquet format to optimize storage and querying.

Challenge 2: Data Type Inconsistencies
•	Problem: Spark sometimes encounters schema mismatches (e.g., IntegerType vs. LongType) when reading and combining Parquet files.
•	Solution: Define and enforce explicit data types for each column in the initial Parquet files.
Challenge 3: Optimizing Fact Table Design
•	Problem: The data warehouse stores years of data, potentially impacting query performance.
•	Solution: Partition fact tables by year and month to improve query efficiency and scalability

**FLOW DIAGRAM**

<img src="https://github.com/user-attachments/assets/a13ccbfb-ecef-47bf-ae42-f648e359f62d" alt="recording_project_1_final" width="355" height="723"/>



