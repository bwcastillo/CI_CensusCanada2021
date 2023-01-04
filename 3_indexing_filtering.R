
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
test<-split.data.frame(georows,georows$Line.Number) %>% 
  map(.,~as.data.frame(.$Line.Number+index)-2) %>% 
  bind_rows()


# New method --------------------------------------------------------------

index<-c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607)#14

# Index -------------------------------------------------------------------

bigIndex #Is a Index created with the 1624 variables with ID's from 165 to 2623, stored in input/bigIndex.xlsx, worked in the final script

bigIndex_of<-data.frame(id=1:length(bigIndex),iddb=bigIndex)

index<-bigIndex_of[bigIndex_of$iddb%in%index,]

#Testing the index for the old method  
test<-split.data.frame(georows,data.frame(id=1:nrow(georows),georows)$id) %>% 
  map(.,~as.data.frame(.$Line.Number+index$id)-2)

test<-test %>% bind_rows(.) #Disorder in the index position, is not sequencial

# index+1624*1
# index+1624*2

vector <-c()
for (i in 1:63409){ #102976216/1624
  a<-index$id+1624*i
  vector[[i]]<-a
}

vec <- Reduce(c,vector)


# Querying the variables --------------------------------------------------

dbQuery<-dbSendQuery(conn,paste("SELECT * 
                             FROM censos.census2021_ci 
                             WHERE",paste('id IN (',
                                          paste(vec, collapse=",")
                             ),") ORDER BY ID;"
)
)

gc()
offResults<-dbFetch(dbQuery)

q1<- paste("SELECT * FROM censos.census2021_ci WHERE",paste('id IN (',
                                          paste(vec, collapse=",")),") ORDER BY ID;"
)
write.txt()
# Index -------------------------------------------------------------------
