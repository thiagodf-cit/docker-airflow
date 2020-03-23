IF  NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'airflow_db')
    CREATE DATABASE [airflow_db]