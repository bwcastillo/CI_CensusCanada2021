#SMALL TEST: Knowing how to make the queries ------------------------------------------
ctv::install.views("HighPerformanceComputing", coreOnly = T)
install.packages("rgpu")
# Knowing rows  -----------------------------------------------------------


#Values position variable of interest
index<-c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607)#14

#Reading where are the geocode row index 

names_georow<-dir(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV"))[2:7]

georows<-lapply(names_georow, function(x){read.csv(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV/",x))}) %>% bind_rows()

georows<-georows[order(georows$Line.Number),] 
#geocode<-lapply(georows, function(x){x[nchar(as.character(x$Geo.Code))>7,]}) %>% bind_rows()

#https://stackoverflow.com/questions/7060272/split-up-a-dataframe-by-number-of-rows



# Old method --------------------------------------------------------------

#Expanding the index creating sums among each of the index plus the 16 variables
# test<-split.data.frame(georows,georows$Line.Number) %>% 
#   map(.,~as.data.frame(.$Line.Number+index)-2) %>% 
#   bind_rows()


# New method --------------------------------------------------------------

index<-c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607)#14

# Index -------------------------------------------------------------------

bigIndex<- dbSendQuery(conn, "SELECT id, characteristic_id FROM  censos.atlantic ORDER BY ID")
bigIndex<- dbFetch(bigIndex) #
bigIndex$characteristic_id <- as.numeric(bigIndex$characteristic_id)

bigIndex <- bigIndex[1:1624,]
bigIndex_of<-data.frame(id=1:length(bigIndex$id),iddb=bigIndex$characteristic_id) #I little bit redundant but change name
index<-bigIndex_of[bigIndex_of$iddb%in%index,]



#Testing the index for the old method  
# test<-split.data.frame(georows,data.frame(id=1:nrow(georows),georows)$id) %>% 
#   map(.,~as.data.frame(.$Line.Number+index$id)-2)
# 
# test<-test %>% bind_rows(.) #Disorder in the index position, is not sequencial

# index+1624*1
# index+1624*2


# Querying the variables --------------------------------------------------

# dbQuery<-dbSendQuery(conn,paste("SELECT * 
#                              FROM censos.census2021_ci 
#                              WHERE",paste('id IN (',
#                                           paste(vec, collapse=",")
#                              ),") ORDER BY ID;"
# )
# )
# 
# gc()
# offResults<-dbFetch(dbQuery)
# 
# q1<- paste("SELECT * FROM censos.census2021_ci WHERE",paste('id IN (',
#                                           paste(vec, collapse=",")),") ORDER BY ID;"
# )
# write.txt()

# Appendix -------------------------------------------------------------------

#Instead Big Index I can query to the database for that column, FEBRUARY 2023 NEW FINDING, LOL!

bigIndex<- dbSendQuery(conn, "SELECT id, characteristic_id FROM  censos.atlantic ORDER BY ID")
bigIndex<- dbFetch(bigIndex)

bigIndex <- bigIndex[index$id,]
