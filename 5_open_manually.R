
ontario<-read.csv("input/98-401-X2021006CI_eng_CSV/csv_ddbb/98-401-X2021006CI_English_CSV_data_Ontario.csv")

gc()
indexOntario<-c()
for (i in 1:nrow(georows[[3]])){ #102976216/1624
  a<-index$id+1624*i
  vector[[i]]<-a
}
gc()
vec <- Reduce(c,vector)
gc()
ontario<-ontario[vec,]

write.csv("output/1raw_datasets/ontario-raw.csv")