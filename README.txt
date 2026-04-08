PROJECT: Decision Support System
GROUP: 13
SCOPE: Final Deadline (Assignments 1-22)

================================================================================
PROJECT STRUCTURE & CONTENT
================================================================================
This repository contains the complete implementation of the BI pipeline, organized
by architectural layer:

[00_data] - DATA STORAGE LAYER
  Contains the dataset at various stages of the pipeline:
  - Read the file README.txt you find inside this folder to understand the various files.

[01_src] - DATA PREPARATION (Python)
  Source code for Data Profiling, Cleaning, and Integration.
  These scripts are responsible for transforming raw semi-structured data 
  into structured formats compliant with the target schema.

[02_sql] - RELATIONAL SCHEMA LAYER (T-SQL)
  Contains the DDL scripts to generate the SQL Server Data Warehouse.
  - A04_DWSchema.sql: Defines the idempotent schema (DROP/CREATE) for 
    Fact Tables, Dimensions, and Bridge Tables.

[03_ssis] - ETL LAYER (Visual Studio / SSIS)
  The Integration Services project ('Group13_SSIS_Project').
  It manages the Control Flow and Data Flow tasks to populate the SQL database
  from the source CSVs.

[04_mdx] - OLAP LAYER (Visual Studio / SSAS)
  The Analysis Services Multidimensional project ('Group13_SSAS').
  It defines the Cube structure (Assignment 14), including:
  - Dimensions and Hierarchies.
  - Measure Groups and Relationships.
  This layer also includes the MDX queries developed to address
  Assignments 15–19

[05_dashboards] – VISUAL ANALYTICS LAYER (Power BI)
  This folder contains the Power BI dashboards built on top of the SSAS cube,
  covering Assignments 20–22.



================================================================================
ARCHITECTURAL NOTE: CUBE DESIGN
================================================================================
The OLAP Cube (Assignment 14) features a hybrid measure design to support both 
standard and weighted analytics:

1. Standard Measure Group ("FactSongStreams"): 
   Linked via standard Star Schema relationships for general reporting.

2. Weighted Measure Group ("PseudoFact_Weighted"): 
   A specialized measure group implemented via Data Source View (Named Query). 
   This architectural component was designed to support complex weighted calculations 
   (e.g., distinguishing Main vs. Featured artist impact: 0.8 vs 0.2 weight) 
   without requiring runtime Many-to-Many logic in MDX queries.