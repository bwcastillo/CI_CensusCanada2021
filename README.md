# Exctracting variables of Interest for Confidence Interval Census 2021 Canada - Dissemination Area scales

Why to use Confidence Interval [Understanding Confidence Intervals (CI)](https://www12.statcan.gc.ca/census-recensement/2021/ref/98-20-0001/982000012021003-eng.cfm)

[Downloading datasets from](https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/details/download-telecharger.cfm?Lang=E)


### Part 1: Creating the environment to exctract data.

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

#### Creating a weird index

It was necessary to build a weird index, but... what it means? It is a weird index because to exctract the variables in each database is necessary reference to its id (*SERIAL PRIMARY KEY*) and name description. The problem is that these identifiers are not ordered, if you realize the variables in the file `98-401-X2021006CI_English_meta.txt` the 1624 variables are nummered from 126 to 2623... ok, what is the problem with this? If you sustract 126 to 2623 is equal to 2497, damn! So what it was done, was copied the number descriptor of the variables in the files mentioned before in an excel file and I added a comma, in that way I could exctract them in the database. So once we get this original index it means from 126 to 2623 and we add a column with the index from 1 to 1624 beside the original one, so in that way we could know what is the original number of each variable. People can say that is better to select the original Id, identify where are the breaks and create and index from there, or maybe query for the name of the variable, but those are another alternatives to explore.  

#### Subseting the index

We just indetified 14 variables of interest

|N°| N° Index Original  | N° Index Database (PostgreSQL) |  Variable Description| 
|:------------- |:-------------:|:-------------:| :-----:|
|1| 135 | 10 |**Number of government transfers recipients aged 15 years and over in private households in 2020 - 25% sample data** |
|2| 1416 | 417|Total - Private households by tenure - 25% sample data (50) - **Renter**|
|3| 1439 | 440|Total - Private households by housing suitability - 25% sample data (55) - <br> **Not suitable**|
|4| 1441 | 442|Total - Occupied private dwellings by period of construction - 25% sample data (56) - <br>**1960 or before**|
|5| 1451 | 452|Total - Occupied private dwellings by dwelling condition - 25% sample data (58) - <br>**Major repairs needed**|
|6| 1467 | 468|Total - Owner and tenant households with household total income greater than zero, in non-farm, non-reserve private dwellings by shelter-cost-to-income ratio - 25% sample data (61) -<br> **Spending 30% or more of income on shelter costs**|
|7| 1488 | 489|Total - Owner households in non-farm, non-reserve private dwellings - 25% sample data (66) -<br> **Median value of dwellings ($)** |
|8| 1536 | 537|Total - Immigrant status and period of immigration for the population in private households -<br> **25% sample data (79) - 2016 to 2021** |
|9| 1695 | 696|Total - Visible minority for the population in private households - 25% sample data (117) -<br>  **Multiple visible minorities**|
|10| 1976 | 977|Total - Mobility status 1 year ago - 25% sample data (163) -<br> **Movers**|
|11| 1999 | 1000|Total - Highest certificate, diploma or degree for the population aged 15 years and over in private households - 25% sample data (165) -<br> **No certificate, diploma or degree**|
|12| 2226 | 1227|Total - Population aged 15 years and over by labour force status - 25% sample data (184) -<br> **Unemployed**|
|13| 2227 | 1228|Total - Population aged 15 years and over by labour force status - 25% sample data (184) -<br> **Not in the labour force**|
|14| 2607 | 1608|Total - Main mode of commuting for the employed labour force aged 15 years and over with a usual place of work or no fixed workplace address - 25% sample data (200) -<br> **Public transit**|

#### Obtaining the Dissemination Areas 
The dataset not include just the Dissemination Areas if not another types of aggregations. So with the file that cointains the positions of each geographic area with extract the Dissemination Areas. 
```

names_georow<-dir(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV"))[2:7]

georows<-lapply(names_georow, function(x){read.csv(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV/",x))}) %>% bind_rows()

georows<-georows[order(georows$Line.Number),] 
```

#### Creating an index 

```
bigIndex<- dbSendQuery(conn, "SELECT id, characteristic_id FROM  censos.atlantic ORDER BY ID")
bigIndex<- dbFetch(bigIndex) #
bigIndex$characteristic_id <- as.numeric(bigIndex$characteristic_id)

bigIndex <- bigIndex[1:1624,]

bigIndex_of<-data.frame(id=1:length(bigIndex),iddb=bigIndex)

index<-bigIndex_of[bigIndex_of$iddb%in%index,]
```

#### Creating an index for each table

```
#Case for each data table
da_position<-lapply(georows,function(x){
  vector <-c() 
  for (i in 1:nrow(x)){ 
          a<-index$id+1624*i
          vector[[i]]<-a
  }
  vector<-c(index$id,vector) #I added a vector because 'a' start to count where the row finish
  vec <- Reduce(c,vector)})

gc()
```

#### Two ways to query, iterative for longer tables, just one query for lighter table
#### First way to query for our variables of interest

Creating a list that contains all the queries for each province
```
dbQuery<-list()
for(i in 1:6){
a<-paste(paste0("SELECT * 
          FROM censos.",provinces[[i]]),"
         WHERE",paste('id IN (',
          paste(da_position[[i]], collapse=",")
                 ),") ORDER BY ID;")
dbQuery[[i]]<-a
}
```


#### Second way, a mutant function that do everything

1. Create chunks, it means that for each Dissemination Area identify what rows need to exctract to get the wanted variables. 
2. After Make the query in two parts
3. We exctract from the list returned what we want
4. From list to data frame
5. Join both data frame
6. Done

```
get_data_chunk<-function(x,y){
  # Doing chunks 
  index<-split(da_position[[x]],ceiling(seq_along(da_position[[x]])/14))


  # Creating queries by each chunk 
  chunks<-lapply(index,function(x){
  paste(paste0("SELECT *
          FROM censos.",y),"
         WHERE",paste('id IN (',
                      paste(x, collapse=",")
         ),") ORDER BY ID;")})


  # Doing and collecting data in two sets through chunks 
  query<-list()
  for (i in 1:10000) {
    x<-dbSendQuery(conn, chunks[[i]])
    x<-dbFetch(x)
    query[[i]]<-x
  }
  
  query_2<-list()
  for (i in 10001:length(index)) {
    x<-dbSendQuery(conn, chunks[[i]])
    x<-dbFetch(x)
    query_2[[i]]<-x
  }

  # Extracting the part of the list of or interest 
  # query_2<-query_2[10001:length(index)-14] #nrow with the original nrow count
  query_2<-query_2[10001:length(index)-1] #nrow with the original nrow count

  #bind rows 
  query<-bind_rows(query)
  query_2<-bind_rows(query_2)

  # Joining the two sets
  q<-rbind(query,query_2)
  
  write.csv(q,paste0("output/1raw_datasets/",y,"-raw.csv"))

  return(q)}
  ```

#### How to query

Its depend of the area 

|N°| Province|Length Dissemination Areas|Length SQL Table| Function to query| Output name in R |
|:--|:-----: |:---:|:---:|:---:|:---:|
|1|Atlantic|5424|8808576|dbSendQuery()|data_a|
|2|British Columbia|8630|14015120|dbSendQuery()|data_bc|
|3|Ontario|21096|34259904|get_data_chunk()|q3|
|4|Prairies|12728|20670272|get_data_chunk()|q4|
|5|Quebec|15188|24665312|get_data_chunk()|q5|
|6|Territories|343|557032|dbSendQuery()|data_t|

#### dbSendQuery()

```
q1<-dbSendQuery(conn, dbQuery[[1]])
data_a<-dbFetch(q1)
write.csv(data_a,paste0("output/1raw_datasets/",provinces[[1]],"-raw.csv"))
rm(data_a,q1)          
gc()

q2<-dbSendQuery(conn, dbQuery[[2]])
data_bc<-dbFetch(q2)
write.csv(data_bc,paste0("output/1raw_datasets/",provinces[[2]],"-raw.csv"))
rm(data_bc,q2)
gc()

q6<-dbSendQuery(conn, dbQuery[[6]])
data_t<-dbFetch(q6)
write.csv(data_t,paste0("output/1raw_datasets/",provinces[[6]],"-raw.csv"))
```


#### get_data_chunk()


```R
q3<-get_data_chunk(4,provinces[[4]])
q4<-get_data_chunk(4,provinces[[4]])
q5<-get_data_chunk(5,provinces[[5]])
```


### Formating 

#### Creating a Veryfier function

We create a function to verify that the variables obtained has keep correct id.
```
veryfier <- function(x){
  x$characteristic_id2<-rep(c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607), nrow(x)/14)
  unique(x$characteristic_id==x$characteristic_id2) #Return true, and not true and false, all right
  
  veryfier(data_a)
veryfier(data_bc)
veryfier(q3)
veryfier(q4)
veryfier(q5)
veryfier(data_t)

}
```

#### 2. Slicing , sticking, wider

##### A. Slicing
1. We create the first it means select the variables of interest Count Total CI, Count Low CI, Count High CI.
2. Changing character as numeric.
3. Unnesting the numeric variables.

##### B. Sticking
1. Slicing to sticker to the Unnested x part of the dataset.
2. We create an column that contains the order of the variables. 
3. One new column that refer to the abbreviation name of each variable is created.
4. The name of the numeric variables of interest are changed.

#### C. Wider
1. We *Pivot Wider* the column that contains the variables.
```R
datascape <-  function(x){
  #Slice
  y <- x
  x<-x[,c(1:13,19,25)]
  x<-mutate_if(x, cols=c(13:15),is.character,as.numeric) 
  x<-unnest(x[,13:15])  #Unnesting converted (char2num) cols

  #Sticker
  y<-y[,c(1:13,19,25,49)]
  y$variables<-rep(c("GOVTRANSFER","RENTER","CROWDHOME", "BUILT1960","REPAIRHOME",
                     "SHLTCOSTR","MEDHOMVAL","RECENTIMMIGRANT","VISMIN_NIE",
                     "MOVERS","NONDEGREE","UNEMPLOYED","NILF","PUBTRANSIT"), nrow(x)/14)
  
    x<-data.frame(y[,c(2:6,16,17)],x) #Adding var to converted

  colnames(x)[7:10] <- c("ID", "TOTAL", "LOW_CI", "HIGH_CI")
  

  #Wider
  x<-pivot_wider(x,names_from=ID,values_from=c(8:10)) #Expanding dataset
  return(x)
}
```
