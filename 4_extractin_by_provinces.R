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
  vector<-c(index$id,vector)
  vec <- Reduce(c,vector)})

gc()


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

# Others ------------------------------------------------------------------


q4<-dbSendQuery(conn, dbQuery[[4]])
data_pr<-dbFetch(q4)

q5<-dbSendQuery(conn, dbQuery[[5]])
data_qc<-dbFetch(q5)

q6<-dbSendQuery(conn, dbQuery[[6]])
data_t<-dbFetch(q6)


#It doesn't work with apply functions
# data<-lapply(dbQuery, function(x){dbSendQuery(conn,x)})
# data<-lapply(data, function(x){dbFetch(x)})




