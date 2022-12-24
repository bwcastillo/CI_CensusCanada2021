
# Old method --------------------------------------------------------------


#Values position variable of interest
index<-c(135,1536,1439,1451,1467,1999,2226,2607,1976,1488,1695,1416,2227,1441)

#Reading where are the geocode row index 

names_georow<-dir(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV"))[2:7]

georows<-lapply(names_georow, function(x){read.csv(paste0(here::here(),"/input/98-401-X2021006CI_eng_CSV/",x))})

lapply(georows, function(x){x[nchar(as.character(x$Geo.Code))>7,]})


#Selecting Dissemination areas
geocode<-geocode[nchar(as.character(geocode$ï..Geo.Code))>7,]

#https://stackoverflow.com/questions/7060272/split-up-a-dataframe-by-number-of-rows

#Expanding the index creating sums among each of the index plus the 16 variables
test<-split.data.frame(geocode,geocode$Line.Number) %>% 
  map(.,~as.data.frame(.$Line.Number+index)-2) %>% 
  bind_rows()



# New method --------------------------------------------------------------

index<-c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607)#14


# index+1624*1
# index+1624*2

vector <-c()
for (i in 1:63409){ #102976216/1624
  a<-index+1624*i
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
