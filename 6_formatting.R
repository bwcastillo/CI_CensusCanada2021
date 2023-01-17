
# Open the saved datasets -------------------------------------------------------
library(replyr)
lapply(provinces, function(x){replyr_nrow(tbl(conn, tolower(x)))/1624*14})

cbind(
data.frame(nrow_set=t(rbind(lapply(list(data_a,data_bc,q3,q4,q5,data_t), function(x){nrow(x)})))),
data.frame(nrow_idx=t(rbind(lapply(georows,function(x){nrow(x)*14})))),
data.frame(length_data=t(rbind(lapply(provinces, function(x){replyr_nrow(tbl(conn, tolower(x)))/1624*14}))))
)#Dataset 4 and 5 are wrong -> Solved in the function for q4 and q5


# Veryfying correct order and not duplicated -----------------------------------------------

library(tidyverse)

colnames(data_a)
prueba<-data_a[,c(1:13,19,25)]
colnames(prueba)

prueba$characteristic_id2<-rep(c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607), nrow(prueba)/14) #BE CAREFUL HERE!
unique(prueba$characteristic_id==prueba$characteristic_id2) #Return true, and not true and false, all right


# slicing, dividing, sticking, widing  ------------------------------------


prueba <- prueba[,1:ncol(prueba)-1]
colnames(prueba)
prueba<-mutate_if(prueba, cols=c(13:15),is.character,as.numeric)
prueba<-unnest(prueba[,13:15])  #Unnesting converted (char2num) cols

prueba2<-data_a[,c(1:13,19,25)]
prueba2$characteristic_id2<-rep(c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607), nrow(prueba)/14)
prueba<-data.frame(prueba2[,c(2:6,16)],prueba) #Adding var to converted
colnames(prueba)[6:9] <- c("ID", "TOTAL", "LOW_CI", "HIGH_CI")
prueba<-pivot_wider(prueba,names_from=characteristic_id2,values_from=c(7:9)) #Expanding dataset


# Creating function to automatize process ---------------------------------

#1. Veryfier

veryfier <- function(x){
  x$characteristic_id2<-rep(c(135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607), nrow(x)/14)
  unique(x$characteristic_id==x$characteristic_id2) #Return true, and not true and false, all right
}

# 135,1416,1439,1441,1451,1467,1488,1536,1695,1976,1999,2226,2227,2607
# c("GOVTRANSFER","RENTER","CROWDHOME", "BUILT1960","REPAIRHOME","SHLTCOSTR","MEDHOMVAL","RECENTIMMIGRANT","VISMIN_NIE",
# "MOVERS","NONDEGREE","UNEMPLOYED","NILF","PUBTRANSIT")
veryfier(data_a)
veryfier(data_bc)
veryfier(q3)
veryfier(q4)
veryfier(q5)
veryfier(data_t)

dataset_ci <- rbind(data_a,data_bc,q3,q4,q5,data_t)

veryfier(dataset_ci)

dataset_ci$characteristic_id[1:14]
#2. Slicing , sticking, wider

datascape <-  function(x){
  #Slice
  y <- x
  x<-x[,c(1:13,19,25)]
  x<-mutate_if(x, cols=c(13:15),is.character,as.numeric)
  x<-unnest(x[,13:15])  #Unnesting converted (char2num) cols
  
  #Sticker
  y<-y[,c(1:13,19,25)]
  y$variables<-rep(c("GOVTRANSFER","RENTER","CROWDHOME", "BUILT1960","REPAIRHOME",
                     "SHLTCOSTR","MEDHOMVAL","RECENTIMMIGRANT","VISMIN_NIE",
                     "MOVERS","NONDEGREE","UNEMPLOYED","NILF","PUBTRANSIT"), nrow(x)/14)
  x<-data.frame(y[,c(2:6,16)],x) #Adding var to converted

  colnames(x)[6:9] <- c("ID", "TOTAL", "LOW_CI", "HIGH_CI")
  
  #Wider
  x<-pivot_wider(x,names_from=ID,values_from=c(7:9)) #Expanding dataset
  return(x)
}

prueba <- datascape(dataset_ci)

#Filtering just Dissemination area, is possible another scales
da2021 <- prueba[prueba$geo_level=="Dissemination area",]
da2021 <- prueba[unique(prueba$dguid),]

#boundaries21<-sf::st_read("C:\\CEDEUS\\2022\\oct03_census2021\\input\\boundaries_21DA\\lda_000b21a_e\\lda_000b21a_e.shp")
#boundaries21 == 57932 rows

#rm(prueba,boundaries21, prueba2, da2021,dataset_ci)