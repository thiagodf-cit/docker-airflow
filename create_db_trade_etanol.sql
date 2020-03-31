DROP TABLE IF EXISTS etanol

CREATE TABLE [etanol]
(
    date_trade varchar (50) NOT NULL,
    value_per_liter_brl varchar (50) NOT NULL,
    value_per_liter_usd varchar (50) NOT NULL,
    weekly_variation varchar (50) NOT NULL,
);

SELECT COUNT(*) FROM [dbo].[etanol]

SELECT * FROM [dbo].[etanol]

DELETE FROM [dbo].[etanol]

INSERT INTO [etanol](date_trade, value_per_liter_brl, value_per_liter_usd, weekly_variation)
VALUES ('20/03/2020', '2.02', '0.3979', '-6.20')
INSERT INTO [etanol](date_trade, value_per_liter_brl, value_per_liter_usd, weekly_variation)
VALUES ('21/03/2020', '1.02', '1.3979', '0.20')