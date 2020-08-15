# StockDataAnalysis
The project used to analyze stock info for Shanghai and Shenzhen Stock Market

Developed under PyCharm

# Prerequsite:
you will need to initialize the development environment with the following programs.

Click-house 

sudo apt-get install apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4
echo "deb https://repo.clickhouse.tech/deb/stable/ main/" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update
sudo service clickhouse-server start
clickhouse-client --password=ABCDE

pip
sudo apt install python-pip
sudo apt install pipenv

click-house driver
pip install clickhouse-driver

MySQL
sudo apt update
sudo apt-get install mysql-server
sudo /usr/bin/mysql_secure_installation

Validatepassword:
pass: ABCDE000
sudo mysql -u root -p
CREATE USER 'admin'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost' WITH GRANT OPTION;

mysql mysql-connector

download data file from BaiduYun


# Current working
import data under clickhouse
decide use csv or line by line import

# TODO:
 
 convert mysql table create into clickhouse with correct type conversion

 add logging functionality for debug
 implement clickhouse system.
 issue 2 testing
 
 issue 3 talib 
 
 write test class and self check


# MAJOR ISSUES


# Update History

 2020-07-12
 verify mySQL work in linux: connect() import()

 2020-07-07
 update table definition clickhouse
 

 2020-07-05
 Connect clickhouse within PyCharm Environment
 install MySQL setup user connect within PyCharm Environment
 add logging stuff
 modified init logic for dbreset()
 

 2020-06-10
 Issue 1 complete, may need more testing or automated testing
 issue 1 achieve single stock function, add functionality for stock list
 issue 1 add functionality to select multiple data column currently price and volumn
 issue 2 kbar all measures


 2020-06-05
 create while loop for option selection
 refreacor initialization part
 change to read 10 line of each imported file
 change table column to fit the actural import file
 hq_snap_pledge: table definition and data column does not match
 hq_snap_spot: table definition and data column does not match


 2020-02-12 1630-1830
 adjust INT(64) into BIGINT during table setup ** for Mysql only\
 single row insert test for hq_index
 COLUMN TransactionTime need BIGINT instead of N(10)
 COLUMN TradeTime need BIGINT instead of N(10)
 using string statement to replace string 'NULL' value
 Major Problem Discovered:
   mismatch between table definition and data
   -hq_snap_pledge
   -hq_snap_spot
 Temp Solution: exclude hq_snap_pledge hq_snap_spot data file during import


 Archive:


 2020-02-11 730-930
 finish shenzhen table setup
 test initialize database and table setup
 prepare insert statement

 2020-02-05 7am-9am
 create database shenzhendata
 setup and create table according to documentation
 up to hq_index

 2020-02-04 6am-8am
 read file from directory
 parse file from directory
 create database, create table, test insert and select
