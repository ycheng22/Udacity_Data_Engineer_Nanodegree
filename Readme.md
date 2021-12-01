**Udacity_Data_Engineer_Nanodegree**

This is a course from [Udacity](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

**Contents:**
- [Part 1: Welcome to the Nanodegree Program](#part-1-welcome-to-the-nanodegree-program)
- [Part 2: Data Modeling](#part-2-data-modeling)
  - [2.1 Introduction to data modeling](#21-introduction-to-data-modeling)
  - [2.2 Relational data models with PostgreSQL database](#22-relational-data-models-with-postgresql-database)
  - [2.3 Project 1a: Data Modeling with Postgres](#23-project-1a-data-modeling-with-postgres)
  - [2.4 NoSQL data models with Apache Cassandra](#24-nosql-data-models-with-apache-cassandra)
  - [2.5 Project 1b: Data Modeling with Apache Cassandra](#25-project-1b-data-modeling-with-apache-cassandra)
- [Part 3: Cloud Data Warehouses](#part-3-cloud-data-warehouses)
- [Part 4: Data Lakes with Spark](#part-4-data-lakes-with-spark)
- [Part 5: Data Pipelines with Airflow](#part-5-data-pipelines-with-airflow)
- [Part 6: Capstone Project](#part-6-capstone-project)

## Part 1: Welcome to the Nanodegree Program 

## Part 2: Data Modeling 

This part includes: 

- Learning relational data models with PostgreSQL database. Learning NoSQL data models with Apache Cassandra.
    
- Project 1a: Data Modeling with Postgres. Modeling user activity data to create a database and ETL pipeline in Postgres for a music streaming app.
    
- Project 1b: Data Modeling with Apache Cassandra. Modeling event data to create a non-relational database and ETL pipeline for a music streaming app.

`Trick`: I use Windows 10, so installing Apache Cassandra could be tricky. I tried on my laptop and desktop. I installed it successfully on my laptop.

### 2.1 Introduction to data modeling

1. What is data modeling?
   <p align="center">
    <img src="./images/P2M1_L1/p2m1l1_dm_1.png"  >
    </p>

2. Intro to relational database
    <p align="center">
    <img src="./images/P2M1_L1/p2m1l1_5_intro_rd_1.png"  >
    <img src="./images/P2M1_L1/p2m1l1_5_intro_rd_2.png"  >
    <img src="./images/P2M1_L1/p2m1l1_5_intro_rd_3.png"  >

    <img src="./images/P2M1_L1/p2m1l1_5_intro_rd_4.png"  >
    </p>

3. When to use relational database?
    <p align="center">
    <img src="./images/P2M1_L1/p2m1l1_6_pros_rd.png"  >
    </p>

4. ACID transactions:
   <p align="center">
    <img src="./images/P2M1_L1/p2m1l1_7_acid.png"  >
    </p>

5. when not to use relational database?
   <p align="center">
    <img src="./images/P2M1_L1/p2m1l1_8_not_use_rd.png"  >
    </p>

6. Introduction to NoSQL database
    <p align="center">
    <img src="./images/P2M1_L1/p2m1l1_14_nosql_1.png"  >

    <img src="./images/P2M1_L1/p2m1l1_14_nosql_2.png"  >
    </p>

7. When to use NoSQL database?
   <p align="center">
    <img src="./images/P2M1_L1/p2m1l1_16_prons_nosql.png"  >
    </p>

8. When not to use NoSQL database?
   <p align="center">
    <img src="./images/P2M1_L1/p2m1l1_16_nouse_nosql.png"  >
    </p>

    There are some NoSQL databases that offer some form of ACID transaction, Such as MongoDB, MarkLogic.

9. The basics of Apache Cassandra
    <p align="center">
    <img src="./images/P2M1_L1/p2m1l1_15_basic_ac_1.png"  >
    <img src="./images/P2M1_L1/p2m1l1_15_basic_ac_2.png"  >
    </p>

### 2.2 Relational data models with PostgreSQL database

1. OLAP vs OLTP
   <p align="center">
    <img src="./images/P2M1_L2/OLAP_OLTP.png"  >
    </p>

2. Normalization
    <p align="center">
    <img src="./images/P2M1_L2/normalization.png"  >
    </p>
3. Normal Form
   <p align="left">
    <img src="./images/P2M1_L2/nm_1.png"  >
    <img src="./images/P2M1_L2/nm_2.png"  >
    <img src="./images/P2M1_L2/1nf.png"  >
    <img src="./images/P2M1_L2/2nf.png"  >
    <img src="./images/P2M1_L2/3nf.png"  >
    </p>

4. Denormalization
   <p align="center">
    <img src="./images/P2M1_L2/denorm.png"  >
    </p>

5. Fact and Dimension Tables 
   <p align="center">
    <img src="./images/P2M1_L2/fact.png"  >
    <img src="./images/P2M1_L2/dim.png"  >
    </p>

6. Star Schema vs Snowflake Schema
- Star Schema
  <p align="left">
    <img src="./images/P2M1_L2/star_s_1.png"  >
    <img src="./images/P2M1_L2/star_s_2.png"  >
    <img src="./images/P2M1_L2/star_s_3.png"  >
    </p>

- Snowflake Schema
  <p align="left">
    <img src="./images/P2M1_L2/snow_s_1.png"  >
    <img src="./images/P2M1_L2/snow_s_2.png"  >
    </p>

- Comparison
  <p align="center">
    <img src="./images/P2M1_L2/star_snow.png"  >
    </p>

7. Some important constraints and clauses
- NOT NULL
- UNIQUE
- PRIMARY KEY
- ON CONFLICT DO NOTHING
- ON CONFLICT DO UPDATE

### 2.3 Project 1a: Data Modeling with Postgres

Check the blog [here](https://ycheng22.github.io/Data_Modeling_with_PostgreSQL/)

### 2.4 NoSQL data models with Apache Cassandra

1. When to use NoSQL
    <p align="center">
    <img src="./images/P2M1_L3/use_nosql.png"  >
    </p>

2. Apache Cassandra
   <p align="center">
    <img src="./images/P2M1_L3/apache_cassandra.png"  >
    </p>

3. CAP Theroem
   <p align="left">
    <img src="./images/P2M1_L3/cap_1.png"  >
    <img src="./images/P2M1_L3/cap_2.png"  >
    </p>

    According to the CAP theorem, a database can actually only guarantee two out of the three in CAP. So supporting Availability and Partition Tolerance makes sense, since Availability and Partition Tolerance are the biggest requirements. 

4. Denormalization
   <p align="left">
    <img src="./images/P2M1_L3/denorm_1.png"  >
    <img src="./images/P2M1_L3/denorm_2.png"  >
    <img src="./images/P2M1_L3/denorm_3.png"  >
    </p>

5. CQL
   <p align="center">
    <img src="./images/P2M1_L3/CQL.png"  >
    </p>

6. Primary Key
   <p align="left">
    <img src="./images/P2M1_L3/primary_key_1.png"  >
    <img src="./images/P2M1_L3/primary_key_2.png"  >
    <img src="./images/P2M1_L3/primary_key_3.png"  >
    <img src="./images/P2M1_L3/primary_key_4.png"  >
    <img src="./images/P2M1_L3/primary_key_5.png"  >
    </p>

7. Clustering Columns
   
	- The clustering column will sort the data in sorted **ascending** order, e.g., alphabetical order. Note: this is a mistake in the video, which says descending order. 
	- More than one clustering column can be added (or none!) 
    - From there the clustering columns will sort in order of how they were added to the primary key 
    <p align="left">
    <img src="./images/P2M1_L3/cluster_col_1.png"  >
    <img src="./images/P2M1_L3/cluster_col_2.png"  >
    </p>

8. Where Clause
    <p align="left">
    <img src="./images/P2M1_L3/where_1.png"  >
    <img src="./images/P2M1_L3/where_2.png"  >
    <img src="./images/P2M1_L3/where_3.png"  >
    </p>

### 2.5 Project 1b: Data Modeling with Apache Cassandra

Check the blog [here](#).

## Part 3: Cloud Data Warehouses 

**Content**: 

- Learning data warehouse on AWS.
    
- Project: Data Warehouse. Extracting data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team. 
    
`Trick`: All functions needed in this course come with free trial on AWS.

## Part 4: Data Lakes with Spark 

**Content**: 

- Learning Spark with AWS.
    
- Project: Data Lake. Building a data lake and an ETL pipeline in Spark that loads data from S3, processes the data into analytics tables, and loads them back into S3. 
    
`Trick`: I installed PySpark on windows 10. PySpark can work, but will pop up warning or wrong message. I switched to Google Colab, which worked withoug pain.

## Part 5: Data Pipelines with Airflow 

**Content**: 

- Learning data piplines on Apache Airflow.
    
- Project: Data Pipelines. Creating and automating a set of data pipelines with Airflow, monitoring and debugging production pipelines.

## Part 6: Capstone Project 



