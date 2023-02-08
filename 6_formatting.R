
# Veryfying the saved datasets -------------------------------------------------------
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

dataset_ci <- rbind(data.frame(data_a,province="Atlantic"),
                    data.frame(data_bc,province="British Columbia"),
                    data.frame(q3,province="Ontario"),
                    data.frame(q4,province="Praires"),
                    data.frame(q5,province="Quebec"),
                    data.frame(data_t,province="Territories"))

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

prueba <- datascape(dataset_ci)

#Filtering just Dissemination area, is possible another scales
da2021 <- prueba[prueba$geo_level=="Dissemination area",]
#da2021 <- prueba[unique(prueba$dguid),]

boundaries21<-sf::st_read("C:\\CEDEUS\\2022\\oct03_census2021\\input\\boundaries_21DA\\lda_000b21a_e\\lda_000b21a_e.shp")
#boundaries21 == 57932 rows


da2021$dguid[!(da2021$dguid%in%unique(boundaries21$DGUID))]
#rm(prueba,boundaries21, prueba2, da2021,dataset_ci)



# Same function for rates -------------------------------------------------


#3. Slicing , sticking, wider RATES

datascape_rates <-  function(x){
  #Slice
  y <- x
  x<-x[,c(1:12,31,37,43)]
  x<-mutate_if(x, cols=c(13:15),is.character,as.numeric)
  x<-unnest(x[,13:15])  #Unnesting converted (char2num) cols
  
  #Sticker
  y<-y[,c(1:12,31,37,43,49)]
  y$variables<-rep(c("GOVTRANSFER","RENTER","CROWDHOME", "BUILT1960","REPAIRHOME",
                     "SHLTCOSTR","MEDHOMVAL","RECENTIMMIGRANT","VISMIN_NIE",
                     "MOVERS","NONDEGREE","UNEMPLOYED","NILF","PUBTRANSIT"), nrow(x)/14)
  
  x<-data.frame(y[,c(2:6,16,17)],x) #Adding var to converted
  
  colnames(x)[7:10] <- c("ID", "TOTAL", "LOW_CI", "HIGH_CI")
  
  
  #Wider
  x<-pivot_wider(x,names_from=ID,values_from=c(8:10)) #Expanding dataset
  return(x)
}

prueba_rate <- datascape_rates(dataset_ci)

da2021_rate <- prueba_rate[prueba_rate$geo_level=="Dissemination area",]



# Saving the datasets -----------------------------------------------------

write.csv(da2021,"C:\\CEDEUS\\2022\\dec01_bbddSina_KasraPaper\\output\\3csv\\query21da_ci.csv")
write.csv(da2021_rate,"C:\\CEDEUS\\2022\\dec01_bbddSina_KasraPaper\\output\\3csv\\query21da_cirate.csv")

writexl::write_xlsx(da2021,"C:\\CEDEUS\\2022\\dec01_bbddSina_KasraPaper\\output\\2excel\\query21da_ci.xlsx")
writexl::write_xlsx(da2021_rate,"C:\\CEDEUS\\2022\\dec01_bbddSina_KasraPaper\\output\\2excel\\query21da_cirate.xlsx")




# Adding variables from regular dataset -----------------------------------



# Selecting for Kasra and Sina CI paper -----------------------------------
# 
# query21da_ks <- read.csv("C:/CEDEUS/2023/jan02_LitonCensus_var/output/formated/csv/query21da_form.csv")
# 
# colnames(query21da_ks)[2:12]
# 
# c("TOTPOP","TOTDWELL","POPDENSITY","BELOW15","SENIOR","FEMALE","ONEPERSONHH",
#   "NOLANG","LOWINCOME","LOWINCSENIOR","LONEPARENT","MEDHHINC","APT5STORY")
# 
# 
# query21da_ks <- query21da_ks[,c(colnames(query21da_ks)[2:12],"TOTPOP","TOTDWELL","POPDENSITY","BELOW15","SENIOR","FEMALE","ONEPERSONHH",
#                "NOLANG","LOWINCOME","LOWINCSENIOR","LONEPARENT","MEDHHINC","APT5STORY")]
# 
# 
# write.csv(query21da_ks,"C:\\CEDEUS\\2022\\dec01_bbddSina_KasraPaper\\output\\3csv\\query21da_100.csv")
# writexl::write_xlsx(query21da_ks,"C:\\CEDEUS\\2022\\dec01_bbddSina_KasraPaper\\output\\2excel\\query21da_100.xlsx")


