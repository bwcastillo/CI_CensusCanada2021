
# Open the saved datasets -------------------------------------------------------

cbind(
data.frame(nrow_set=t(rbind(lapply(list(data_a,data_bc,q3,q4,q5,data_t), function(x){nrow(x)})))),
data.frame(nrow_idx=t(rbind(lapply(georows,function(x){nrow(x)*14})))),
data.frame(length_data=t(rbind(lapply(provinces, function(x){replyr_nrow(tbl(conn, tolower(x)))/1624*14}))))
)#Dataset 4 and 5 are wrong -> Solved in the function for q4 and q5


# Veryfying length datasets -----------------------------------------------
library(replyr)
lapply(provinces, function(x){replyr_nrow(tbl(conn, tolower(x)))/1624*14})
