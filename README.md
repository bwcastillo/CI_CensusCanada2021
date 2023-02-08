# Exctracting variables of Interest for Confidence Interval Census 2021 Canada - Dissemination Area scales

Why to use Confidence Interval [Understanding Confidence Intervals (CI)](https://www12.statcan.gc.ca/census-recensement/2021/ref/98-20-0001/982000012021003-eng.cfm)

[Downloading datasets from](https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/details/download-telecharger.cfm?Lang=E)


###Part 1: Creating the environment to exctract data.

#### Setting up directories

Create input folder extract the file `98-401-X2021006CI_eng_CSV` create a folder `csv_ddbb` and store the csv files  
`input/98-401-X2021006CI_eng_CSV/csv_ddbb/`

#### Establishing conection with Postgres SQL 

```
fun_connect<-function(){dbConnect(RPostgres::Postgres(),
                                  dbname='censos',
                                  host='localhost',
                                  port=5432,
                                  user='postgres',
                                  password='adminpass',
                                  options= '-c search_path=censos')}

conn<-fun_connect()

```
#### Getting the file names

```
file_name<-dir(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV/csv_ddbb")[1:6])
provinces<-sub(".*data_", "", file_name)
provinces<-sub(".7z*", "", provinces) 
```

#### Creating tables queries
We store the queries for each dataset through `lapply` function using the names stores in the above step. 

```
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
````

#### Sending queries to create tables
````
dbSendQuery(conn,create_table[[1]])#Atlantic
dbSendQuery(conn,create_table[[2]])#BritishColumbia
dbSendQuery(conn,create_table[[3]])#Ontario
dbSendQuery(conn,create_table[[4]])#Prairies
dbSendQuery(conn,create_table[[5]])#Quebec
dbSendQuery(conn,create_table[[6]])#Territories
````

#### Loading tables with data

Creating the query to load the data. One database query for each list element. 
Loading with `dbSendQuery` it doesn't work with `lapply()`

```
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
          FROM PROGRAM '7z e -so", paste0(here::here(), "/input/98-401-X2021006CI_eng_CSV/csv_ddbb/"),provinces_file$file[i],"' DELIMITER ',' CSV HEADER encoding 'windows-1251';")
load_data[[i]]<-b
}
````

#### Counting how many rows has each database

Counting and contrasting with the original excel file that counts the rows

```
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
```

#### Pause: comments

I tried to unite the table and do the query from there but it results very heavy for the computer. To query a table with this in a bigger computer is possible to use Amazon Web Services how is explained in [this tutorial](https://github.com/bwcastillo/doc-postgresqlandr) 
Is possible to see how to join the tables and when I tried to exctract information in there, in the file `1_connection_setup.R` from the line 180 to the end of the code. The query where I tried to exctract the data is in the file `2_bigQuery.R`.


### Part2: Exctracting data

#Creating a weird index

It was necessary to build a weird index, but... what it means? It is a weird index because to exctract the variables in each database is necessary reference to its id and name description. The problem is that these identifiers are not ordered, if you realize the variables in the file `98-401-X2021006CI_English_meta.txt` the 1624 variables are nummered from 126 to 2623... ok, what is the problem with this? If you sustract 126 to 2623 is equal to 2497, damn. So what I did, just I copied the number descriptor of the variables in the files mentioned before in an excel file and I added a comma, in that way I could exctract them in the database. 

