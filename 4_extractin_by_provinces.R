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

# 2. Knowing rows  -----------------------------------------------------------

#Values position variable of interest
index<-c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607)#14

#Reading where are the geocode row index 

names_georow<-dir(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV"))[2:7]

georows<-lapply(names_georow, function(x){read.csv(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV/",x))})

# Creating Index position -------------------------------------------------

bigIndex #Is a Index created with the 1624 variables with ID's from 165 to 2623, stored in input/bigIndex.xlsx, worked in the final script

bigIndex_of<-data.frame(id=1:length(bigIndex),iddb=bigIndex)

index<-bigIndex_of[bigIndex_of$iddb%in%index,]


# Creating Index position for each province -------------------------------

#Case supposing just one data table
vector <-c()
for (i in 1:nrow(georows[[1]])){ #102976216/1624
  a<-index$id+1624*i
  vector[[i]]<-a
}

vec <- Reduce(c,vector)


#Case for each data table
test<-lapply(georows,function(x){
  vector <-c() 
  for (i in 1:nrow(x)){ 
          a<-index$id+1624*i
          vector[[i]]<-a
  }
  vector<-c(index$id,vector) #I added a vector because 'a' start to count where the row finish
  vec <- Reduce(c,vector)})

gc()

#Remember ever time evaluate selecting specific columns SELECT COL1, COL2, ... this time didnt work
dbQuery<-list()
for(i in 1:6){
a<-paste(paste0("SELECT * 
          FROM censos.",provinces[[i]]),"
         WHERE",paste('id IN (',
          paste(test[[i]], collapse=",")
                 ),") ORDER BY ID;")
dbQuery[[i]]<-a
}



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


#Ontario bad guy:
q3<-dbSendQuery(conn, dbQuery[[3]])
gc()
data_on<-dbFetch(q3)

# Ontario Case ------------------------------------------------------------

#See if is working

query_on<-paste(paste0("SELECT *
          FROM censos.",provinces[[i]]),"
         WHERE",paste('id IN (',
                      paste(test[[3]][1:14], collapse=",")
         ),") ORDER BY ID;")


q3<-dbSendQuery(conn, query_on)

data_on<-dbFetch(q3)


# Ontario through chunks ---------------------------------------------------------------


#https://stackoverflow.com/questions/3318333/split-a-vector-into-chunks
# seq_along(test[[3]]) # It creates an index
# ceiling(seq_along(test[[3]])/14) #It creates a chunk-group position across the index

# Doing chunks Ontario ------------------------------------------------------------
index_on<-split(test[[3]],ceiling(seq_along(test[[3]])/14))


# Creating queries by each chunk ------------------------------------------
chunks_on<-lapply(index_on,function(x){
paste(paste0("SELECT *
          FROM censos.",provinces[[3]]),"
         WHERE",paste('id IN (',
                      paste(x, collapse=",")
         ),") ORDER BY ID;")})


# Doing and collecting data in two sets through chunks --------------------
query_on<-list()
for (i in 1:10000) {
  x<-dbSendQuery(conn, chunks_on[[i]])
  x<-dbFetch(x)
  query_on[[i]]<-x
}

query_on2<-list()
for (i in 10001:length(index_on)) {
  x<-dbSendQuery(conn, chunks_on[[i]])
  x<-dbFetch(x)
  query_on2[[i]]<-x
}


# Extracting the part of the list of or interest --------------------------
query_on2<-query_on2[10001:nrow(georows[[3]])] #nrow with the original nrow count


# bind rows ---------------------------------------------------------------
query_on<-bind_rows(query_on)
query_on2<-bind_rows(query_on2)


# Joining the two sets ----------------------------------------------------
q3<-rbind(query_on,query_on2)


# Veryifing same length ---------------------------------------------------
nrow(georows[[3]])*14==nrow(q3) #Same length 
length(test[[3]])-14==nrow(q3)  #Same length - remain that test list index was summed by the initial 14 variables 

# write.csv(query_on,paste0("output/1raw_datasets/",provinces[[3]],"1-raw.csv"))
# write.csv(query_on2,paste0("output/1raw_datasets/",provinces[[3]],"2-raw.csv"))
write.csv(q3,paste0("output/1raw_datasets/",provinces[[3]],"-raw.csv"))

# Others ------------------------------------------------------------------

q4<-dbSendQuery(conn, dbQuery[[4]])
data_pr<-dbFetch(q4)
write.csv(data_pr,paste0("output/1raw_datasets/",provinces[[4]],"-raw.csv"))
q4<-get_data_chunk(4,provinces[[4]])

q5<-dbSendQuery(conn, dbQuery[[5]])
data_qc<-dbFetch(q5)
write.csv(data_qc,paste0("output/1raw_datasets/",provinces[[5]],"-raw.csv"))
q5<-get_data_chunk(5,provinces[[5]])


q6<-dbSendQuery(conn, dbQuery[[6]])
data_t<-dbFetch(q6)
write.csv(data_t,paste0("output/1raw_datasets/",provinces[[6]],"-raw.csv"))

#It doesn't work with apply functions
# data<-lapply(dbQuery, function(x){dbSendQuery(conn,x)})
# data<-lapply(data, function(x){dbFetch(x)})



# Appendix: Function that make everything lol -----------------------------
get_data_chunk<-function(x,y){
  # Doing chunks 
  index<-split(test[[x]],ceiling(seq_along(test[[x]])/14))


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
  query_2<-query_2[10001:length(index)-14] #nrow with the original nrow count

  #bind rows 
  query<-bind_rows(query)
  query_2<-bind_rows(query_2)

  # Joining the two sets
  q<-rbind(query,query_2)
  
  write.csv(q,paste0("output/1raw_datasets/",y,"-raw.csv"))

  return(q)}

