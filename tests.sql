DROP TABLE IF EXISTS airflow_log
    CREATE TABLE [airflow_log]
    (
        title varchar (50) NOT NULL,
        is_valid INT,
        link varchar (50),
        date_valid DATETIME
    );

INSERT INTO [airflow_log](title, is_valid, link)
VALUES ('Title One', 0, 'http://www.link-one.com.br')
INSERT INTO [airflow_log](title, is_valid, link)
VALUES ('Title Two', 1, 'http://www.link-two.com.br')