
library(data.table)
library(RColorBrewer)
library(wordcloud)
library(tidyr)
library(dplyr)
# Load Data ###############################################################################
dat.gram2.businessid.freq <- readRDS("/Users/yanjin1993/GitHub/Personalize-Yelp-Business/output/gram2_businessid_freq.rds")



# select by 
# temp <-dat.gram2.businessid.freq %>%
#   gather(word, freuqnecy, -(business_id)) %>%
#   group_by(business_id) %>%
#   summarise(num_words = n()) 
#   
temp <- dat.gram2.businessid.freq %>% 
  filter(business_id=="4bEjOyTaDG24SY5TxsaUNQ") 

temp <- setDT(as.data.frame(t(temp)), keep.rownames = TRUE)[] 
temp <- temp %>% filter(V1 != 0)
temp <- temp %>% filter(rn != "business_id")
temp <- temp %>% mutate(V1 = as.numeric(V1))

write.csv(temp, "/Users/yanjin1993/GitHub/Personalize-Yelp-Business/output/temp.csv")

# temp is a word freq for business_id <- 4bEjOyTaDG24SY5TxsaUNQ
pal <- brewer.pal(9,"YlGnBu")
pal <- pal[-(1:4)]
set.seed(123)

wordcloud(words = temp$rn, freq = temp$V1,
          scale=c(2,0.2), max.words=300, random.order=FALSE, 
          rot.per=0.35, use.r.layout=FALSE, 
          colors=brewer.pal(8, "Dark2"))



# get all DTM
temp.all <- cbind(as.data.frame(colnames(dat.gram2.businessid.freq[,-1])),
                  as.data.frame(colSums(dat.gram2.businessid.freq[,-1])))
colnames(temp.all) <- c("word", "freq")

wordcloud(words = temp.all$word, freq = temp.all$freq,
          scale=c(5,0.5), max.words=300, random.order=FALSE, 
          rot.per=0.35, use.r.layout=FALSE, 
          colors=brewer.pal(8, "Dark2"))





