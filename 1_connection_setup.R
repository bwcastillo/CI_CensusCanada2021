library(RPostgreSQL)
library(dplyr)
library(tidyverse)
library(DBI)

# 1.Establishing connection -----------------------

fun_connect<-function(){dbConnect(RPostgres::Postgres(),
                                  dbname='censos',
                                  host='localhost',
                                  port=5432,
                                  user='postgres',
                                  password='adminpass',
                                  options= '-c search_path=censos')}

conn<-fun_connect()

# 2.Getting names ----------------
file_name<-dir("C:/CEDEUS/2022/dec01_bbddSina_KasraPaper/input/98-401-X2021006CI_eng_CSV/csv_ddbb")[1:6]
provinces<-sub(".*data_", "", file_name)
provinces<-sub(".7z*", "", provinces) 
# 3.Creating tables ---------------------------------------------------------


create_table<-lapply(provinces,function(x){paste0("CREATE TABLE ",x," (id SERIAL PRIMARY KEY,
CENSUS_YEAR VARCHAR(50),
DGUID VARCHAR(100),
ALT_GEO_CODE VARCHAR(100),
GEO_LEVEL VARCHAR(100),
GEO_NAME VARCHAR(100),
TNR_SF VARCHAR(100),
TNR_LF VARCHAR(100),
DATA_QUALITY_FLAG VARCHAR(100),
CHARACTERISTIC_ID VARCHAR(100),
CHARACTERISTIC_NAME VARCHAR(200),
CHARACTERISTIC_NOTE VARCHAR(200),
C1_COUNT_TOTAL VARCHAR(50),
SYMBOL1 VARCHAR(10),
\"C2_COUNT_MEN+\" VARCHAR(50),
SYMBOL2 VARCHAR(10),
\"C3_COUNT_WOMEN+\" VARCHAR(50),
SYMBOL3 VARCHAR(10),
C4_COUNT_LOW_CI_TOTAL VARCHAR(50),
SYMBOL4 VARCHAR(10),
\"C5_COUNT_LOW_CI_MEN+\" VARCHAR(50),
SYMBOL5 VARCHAR(10),
\"C6_COUNT_LOW_CI_WOMEN+\" VARCHAR(50),
SYMBOL6 VARCHAR(10),
C7_COUNT_HI_CI_TOTAL VARCHAR(50),
SYMBOL7 VARCHAR(10),
\"C8_COUNT_HI_CI_MEN+\" VARCHAR(50),
SYMBOL8 VARCHAR(10),
\"C9_COUNT_HI_CI_WOMEN+\" VARCHAR(50),
SYMBOL9 VARCHAR(10),
C10_RATE_TOTAL VARCHAR(50),
SYMBOL10 VARCHAR(10),
\"C11_RATE_MEN+\" VARCHAR(50),
SYMBOL11 VARCHAR(10),
\"C12_RATE_WOMEN+\" VARCHAR(50),
SYMBOL12 VARCHAR(10),
C13_RATE_LOW_CI_TOTAL VARCHAR(50),
SYMBOL13 VARCHAR(10),
\"C14_RATE_LOW_CI_MEN+\" VARCHAR(50),
SYMBOL14 VARCHAR(10),
\"C15_RATE_LOW_CI_WOMEN+\" VARCHAR(50),
SYMBOL15 VARCHAR(10),
C16_RATE_HI_CI_TOTAL VARCHAR(50),
SYMBOL16 VARCHAR(10),
\"C17_RATE_HI_CI_MEN+\" VARCHAR(50),
SYMBOL17 VARCHAR(10),
\"C18_RATE_HI_CI_WOMEN+\" VARCHAR(50),
SYMBOL18 VARCHAR(10))")})



dbSendQuery(conn,create_table[[1]])#Atlantic
dbSendQuery(conn,create_table[[2]])#BritishColumbia
dbSendQuery(conn,create_table[[3]])#Ontario
dbSendQuery(conn,create_table[[4]])#Prairies
dbSendQuery(conn,create_table[[5]])#Quebec
dbSendQuery(conn,create_table[[6]])#Territories


# 4.Loading data -----------------------------------------------------------

provinces_file<-data.frame(file=file_name,provinces=provinces)


load_data<-list()
for (i in 1:nrow(provinces_file)){
b<-paste0("copy censos.",provinces_file$provinces[i] ," (CENSUS_YEAR,
DGUID,
ALT_GEO_CODE,
GEO_LEVEL,
GEO_NAME,
TNR_SF,
TNR_LF,
DATA_QUALITY_FLAG,
CHARACTERISTIC_ID,
CHARACTERISTIC_NAME,
CHARACTERISTIC_NOTE,
C1_COUNT_TOTAL,
SYMBOL1,
\"C2_COUNT_MEN+\",
SYMBOL2,
\"C3_COUNT_WOMEN+\",
SYMBOL3,
C4_COUNT_LOW_CI_TOTAL,
SYMBOL4,
\"C5_COUNT_LOW_CI_MEN+\",
SYMBOL5,
\"C6_COUNT_LOW_CI_WOMEN+\",
SYMBOL6,
C7_COUNT_HI_CI_TOTAL,
SYMBOL7,
\"C8_COUNT_HI_CI_MEN+\",
SYMBOL8,
\"C9_COUNT_HI_CI_WOMEN+\",
SYMBOL9,
C10_RATE_TOTAL,
SYMBOL10,
\"C11_RATE_MEN+\",
SYMBOL11,
\"C12_RATE_WOMEN+\",
SYMBOL12,
C13_RATE_LOW_CI_TOTAL,
SYMBOL13,
\"C14_RATE_LOW_CI_MEN+\",
SYMBOL14,
\"C15_RATE_LOW_CI_WOMEN+\",
SYMBOL15,
C16_RATE_HI_CI_TOTAL,
SYMBOL16,
\"C17_RATE_HI_CI_MEN+\",
SYMBOL17,
\"C18_RATE_HI_CI_WOMEN+\",
SYMBOL18) 
FROM PROGRAM '7z e -so C:/CEDEUS/2022/dec01_bbddSina_KasraPaper/input/98-401-X2021006CI_eng_CSV/csv_ddbb/",provinces_file$file[i],"' DELIMITER ',' CSV HEADER encoding 'windows-1251';")
load_data[[i]]<-b
}

lapply(load_data, function(x){dbSendQuery(conn,x)})

dbSendQuery(conn,load_data[[1]])#Atlantic
dbSendQuery(conn,load_data[[2]])#BritishColumbia
dbSendQuery(conn,load_data[[3]])#Ontario
dbSendQuery(conn,load_data[[4]])#Prairies
dbSendQuery(conn,load_data[[5]])#Quebec
dbSendQuery(conn,load_data[[6]])#Territories


# 5.Counting how many rows has each dataset ---------------------------------
nrow_query<-list()
for (i in 1:length(provinces)){
query<-dbSendQuery(conn, paste0("SELECT count(*) FROM censos.",tolower(provinces[i])))
nrow_query[[i]]<-dbFetch(query)
}

nrow_query[1]
nrow_query[2]
nrow_query[3]
nrow_query[4]
nrow_query[5]
nrow_query[6]


b<-c()
for (i in 1:length(nrow_query)){
  a<-nrow_query[[i]]$count
  b[[i]]<-a
}

sapply(nrow_query, function(x){(x)})

options(scipen=999)
sum(b)



# Creating table united ---------------------------------------------------

#dbSendQuery(conn, "DROP TABLE censos.census2021_ci")

dbSendQuery(conn, paste("CREATE TABLE census2021_ci AS",
                  paste0("SELECT * FROM censos.",provinces[1]),
                  "UNION ALL",
                  paste0("SELECT * FROM censos.",provinces[2]),  
                  "UNION ALL",
                  paste0("SELECT * FROM censos.",provinces[3]),
                  "UNION ALL",
                  paste0("SELECT * FROM censos.",provinces[4]),
                  "UNION ALL",
                  paste0("SELECT * FROM censos.",provinces[5]),
                  "UNION ALL",
                  paste0("SELECT * FROM censos.",provinces[6]))) 

query<-dbSendQuery(conn, "SELECT count(*) FROM censos.census2021_ci")
nrow_bigddbb<-dbFetch(query)

dbSendQuery(conn, "ALTER TABLE censos.census2021_ci
            ADD COLUMN id2 SERIAL PRIMARY KEY;")


q1<-dbSendQuery(conn, "SELECT id2 FROM censos.census2021_ci")
q1<-dbFetch(q1)

tail(q1, 10)
# Trying to add identifier ------------------------------------------------

dbSendQuery(conn, paste("CREATE TABLE census2021_ci AS
                        SELECT row_number() OVER (ORDER BY id) AS id_2 FROM (",
                        paste0("SELECT * FROM censos.",provinces[1]),
                        "UNION ALL",
                        paste0("SELECT * FROM censos.",provinces[2]),  
                        "UNION ALL",
                        paste0("SELECT * FROM censos.",provinces[3]),
                        "UNION ALL",
                        paste0("SELECT * FROM censos.",provinces[4]),
                        "UNION ALL",
                        paste0("SELECT * FROM censos.",provinces[5]),
                        "UNION ALL",
                        paste0("SELECT * FROM censos.",provinces[6]),") AS foo")) 

q2<-dbSendQuery(conn, "SELECT id_2 FROM censos.census2021_ci")
q2<-dbFetch(q2)

tail(q1, 10)
gc()
