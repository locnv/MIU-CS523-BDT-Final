# Create table stocks

# Date,Open,High,Low,Close,Adj Close,Volume

DROP TABLE stocks;

CREATE EXTERNAL TABLE stocks(StockName String, Date Date,Open FLOAT,High FLOAT,Low FLOAT,Close FLOAT,AdjClose FLOAT,Volume DOUBLE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
LOCATION '/user/cloudera/cs523/final/stock';


# Create TABLE cryptos
DROP TABLE cryptos;

CREATE EXTERNAL TABLE cryptos(cryptoName String, Date DATE,Open FLOAT,High FLOAT,Low FLOAT,Close FLOAT,AdjClose FLOAT,Volume DOUBLE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
LOCATION '/user/cloudera/cs523/final/crypto';
