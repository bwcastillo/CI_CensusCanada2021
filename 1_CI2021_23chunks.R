library(RPostgreSQL)
library(dplyr)
library(tidyverse)
library(DBI)


# 0-1 ---------------------------------------------------------------------


dir.create('input')
dir.create('output')
dir.create('output/1raw_datasets')
dir.create('output/2excel')
dir.create('output/3csv')

options(timeout = max(300, getOption("timeout"))) #I expand the time to keep downloading a file 
download.file('https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/details/download-telecharger/comp/GetFile.cfm?Lang=E&FILETYPE=CSV&GEONO=006',"input/98-401-X2021006CI_eng_CSV.zip", cacheOK=FALSE, mode = 'wb') #Downloading the file
fold1 <- 'input/98-401-X2021006CI_eng_CSV' #Create the address where I will create a folder to unzip the downloaded file
dir.create(fold1)

fold2 <- 'input/98-401-X2021006CI_eng_CSV/csv_ddbb/'
dir.create(fold2)

unzip("input/98-401-X2021006CI_eng_CSV.zip",exdir= fold1)

move <- dir(fold1)[1:6]

lapply(move, function(x){library(filesstrings)
  file.move(paste0(here::here(),'/',fold1,"/",x),
            fold2)})


# 2 -----------------------------------------------------------------------

fun_connect<-function(){dbConnect(RPostgres::Postgres(),
                                  dbname='censos',
                                  host='localhost',
                                  port=5432,
                                  user='postgres',
                                  password='adminpass',
                                  options= '-c search_path=censos')}

conn<-fun_connect()


# 3 -----------------------------------------------------------------------

lapply(move, function(x){
  system(
    paste0('7z a \"', str_replace_all(here::here(),"/","\\\\"),'\\\\', str_replace_all(fold2,"/","\\\\" ),str_remove(x,'.csv'),'.7z\" ',
           paste0('\"',str_replace_all(here::here(),"/","\\\\"),'\\\\', str_replace_all(fold2,"/","\\\\" ), x),'\"'),
    intern=F,
    ignore.stdout = F,
    ignore.stderr = F,
    wait=T)})

# 4 -----------------------------------------------------------------------
lapply(move,function(x){unlink(paste0(here::here(),'/',fold2,x))})


# 5 -----------------------------------------------------------------------
file_name<-dir(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV/csv_ddbb")[1:6])
provinces<-sub(".*data_", "", file_name)
provinces<-sub(".7z*", "", provinces) 



# 8 -----------------------------------------------------------------------

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


# 9 -----------------------------------------------------------------------
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

library(bit64)

counts <- sapply(nrow_query, function(x){x/1})

sum(unlist(t(counts)))



# 10 -----------------------------------------------------------------------

names_georow<-dir(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV"))[2:7]

georows<-lapply(names_georow, function(x){read.csv(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV/",x))})

#georows<-georows[order(georows$Line.Number),] 


# 11 -----------------------------------------------------------------------
index<-c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607)#14

# 12 ----------------------------------------------------------------------

bigIndex<- dbSendQuery(conn, "SELECT id, characteristic_id FROM censos.atlantic ORDER BY ID")
bigIndex<- dbFetch(bigIndex) #
bigIndex$characteristic_id <- as.numeric(bigIndex$characteristic_id)

bigIndex <- bigIndex[1:1624,]

bigIndex_of<-data.frame(id=bigIndex$id,iddb=bigIndex$characteristic_id)


index<-bigIndex_of[bigIndex_of$iddb%in%index,]


# 13 ----------------------------------------------------------------------
da_position<-lapply(georows,function(x){
  vector <-c() 
  for (i in 1:nrow(x)){ 
    a<-index$id+1624*i
    vector[[i]]<-a
  }
  vector<-c(index$id,vector) #I added a vector because 'a' start to count where the row finish
  vec <- Reduce(c,vector)})

gc()




# 14 ----------------------------------------------------------------------


dbQuery<-list()
for(i in 1:6){
  a<-paste(paste0("SELECT * 
          FROM censos.",provinces[[i]]),"
         WHERE",paste('id IN (',
                      paste(da_position[[i]], collapse=",")
         ),") ORDER BY ID;")
  dbQuery[[i]]<-a
}


# 15 ----------------------------------------------------------------------


get_data_chunk<-function(x,y){
  # Doing chunks 
  index<-split(da_position[[x]],ceiling(seq_along(da_position[[x]])/14)) #Change '14' for number of variables
  
  
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


# 16 ----------------------------------------------------------------------
q1<-dbSendQuery(conn, dbQuery[[1]])
data_a<-dbFetch(q1)
write.csv(data_a,paste0("output/1raw_datasets/",provinces[[1]],"-raw.csv"))
#rm(data_a,q1)          
gc()

q2<-dbSendQuery(conn, dbQuery[[2]])
data_bc<-dbFetch(q2)
write.csv(data_bc,paste0("output/1raw_datasets/",provinces[[2]],"-raw.csv"))
#rm(data_bc,q2)
gc()

q6<-dbSendQuery(conn, dbQuery[[6]])
data_t<-dbFetch(q6)
write.csv(data_t,paste0("output/1raw_datasets/",provinces[[6]],"-raw.csv"))



# 17 ----------------------------------------------------------------------

q3<-get_data_chunk(3,provinces[[3]])
q4<-get_data_chunk(4,provinces[[4]])
q5<-get_data_chunk(5,provinces[[5]])



# 18 ----------------------------------------------------------------------

verifier <- function(x){
  x$characteristic_id2<-rep(c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607), nrow(x)/14)
  unique(x$characteristic_id==x$characteristic_id2) #Return true, and not true and false, all right
}
  verifier(data_a)
  verifier(data_bc)
  verifier(q3)
  verifier(q4)
  verifier(q5)
  verifier(data_t)
  
  dataset_ci <- rbind(data.frame(data_a,province="Atlantic"),
                      data.frame(data_bc,province="British Columbia"),
                      data.frame(q3,province="Ontario"),
                      data.frame(q4,province="Praires"),
                      data.frame(q5,province="Quebec"),
                      data.frame(data_t,province="Territories"))
  
  verifier(dataset_ci)
  

# 19 ----------------------------------------------------------------------

  datascape <-  function(x){
    #Slice
    y <- x
    x<-x[,c(1:13,19,25)]  
    x<-mutate_if(x, cols=c(13:15),is.character,as.numeric) 
    x<-unnest(x[,13:15])  #Unnesting converted (char2num) cols
    
    #Sticker
    y<-y[,c(1:13,19,25,49)]  
    y$variables<-rep(c("GOVTRANSFER","RENTER","CROWDHOME", "BUILT1960","REPAIRHOME", #Change the name of variables according you selection
                       "SHLTCOSTR","MEDHOMVAL","RECENTIMMIGRANT","VISMIN_NIE",
                       "MOVERS","NONDEGREE","UNEMPLOYED","NILF","PUBTRANSIT"), nrow(x)/nrow(index))#Change '14' for the number of variables that you chose
    
    x<-data.frame(y[,c(2:6,16,17)],x) #Adding var to converted
    
    colnames(x)[7:10] <- c("ID", "TOTAL", "LOW_CI", "HIGH_CI")
    
    
    #Wider
    x<-pivot_wider(x,names_from=ID,values_from=c(8:10)) #Expanding dataset
    return(x)
  }

# 20 ----------------------------------------------------------------------


  prueba <- datascape(dataset_ci)
  da2021 <- prueba[prueba$geo_level=="Dissemination area",]
  

# 21 ----------------------------------------------------------------------

  download.file('https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lda_000a21a_e.zip',"input/lda_000a21a_e.zip", cacheOK=FALSE, mode = 'wb') #Downloading the file
  dir.create('input/lda_000a21a_e')
  unzip("input/lda_000a21a_e.zip",exdir= 'input/lda_000a21a_e')
  boundaries21<-sf::st_read(paste0(here::here(),"\\input\\lda_000a21a_e\\lda_000a21a_e.shp"))
  
  da2021$dguid[!(da2021$dguid%in%unique(boundaries21$DGUID))] #PERFECT all the Dissemination Areas are included in the boundaries
  

# 22 ----------------------------------------------------------------------

  datascape_rates <-  function(x){
    #Slice
    y <- x
    x<-x[,c(1:12,31,37,43)] 
    x<-mutate_if(x, cols=c(13:15),is.character,as.numeric)
    x<-unnest(x[,13:15])  #Unnesting converted (char2num) cols
    
    #Sticker
    y<-y[,c(1:12,31,37,43,49)]
    y$variables<-rep(c("GOVTRANSFER","RENTER","CROWDHOME", "BUILT1960","REPAIRHOME", #Change the name of variables according you selection
                       "SHLTCOSTR","MEDHOMVAL","RECENTIMMIGRANT","VISMIN_NIE", 
                       "MOVERS","NONDEGREE","UNEMPLOYED","NILF","PUBTRANSIT"), nrow(x)/14) #Change '14' for the number of variables that you chose 
    
    x<-data.frame(y[,c(2:6,16,17)],x) #Adding var to converted
    
    colnames(x)[7:10] <- c("ID", "TOTAL", "LOW_CI", "HIGH_CI")
    
    
    #Wider
    x<-pivot_wider(x,names_from=ID,values_from=c(8:10)) #Expanding dataset
    return(x)
  }
  
  prueba_rate <- datascape_rates(dataset_ci)
  
  da2021_rate <- prueba_rate[prueba_rate$geo_level=="Dissemination area",]
  

# 23 ----------------------------------------------------------------------


  #XLSX
  writexl::write_xlsx(da2021,paste0(here::here(),"\\output\\2excel\\query21da_ci.xlsx"))
  writexl::write_xlsx(da2021_rate,paste0(here::here(),"\\output\\2excel\\query21da_cirate.xlsx"))
  
  #CSV
  write.csv(da2021,paste0(here::here(),"\\output\\3csv\\query21da_ci.csv"))
  write.csv(da2021_rate,paste0(here::here(),"\\output\\3csv\\output\\3csv\\query21da_cirate.csv"))  
  