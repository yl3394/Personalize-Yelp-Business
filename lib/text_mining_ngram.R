# Basic Setup #############################################################################
# load package
library(jsonlite)
library(dplyr)
library(tidyr)
library(NLP)
library(tm)
library(ggplot2)

# set working directory
setwd("/Users/yanjin1993/GitHub/Personalize-Yelp-Business/tm/")
# load DTM created environment 
load("/Users/yanjin1993/Google Drive/Columbia University /2016 Fall /Big Data Analytics/final_project/environment/100_enviornment_dtm_clean.RData")

# Data Loading ###########################################################################
file_path <- "/Users/yanjin1993/Desktop/Restaurants/reviews.json"
# load review raw data 
datraw.review <- fromJSON(txt=file_path)
# make a copy 
dat.review <- datraw.review

# Helper Functions ########################################################################
# get partitioned-data text-mining files 
GetPartitionedDTM <- function(input_data){
  # set up a timer
  ptm <- proc.time()
  # create a return list
  return_list <- c()
  
  doc <- Corpus(VectorSource(input_data$text))
  # status check 
  print("Corpus generated!")
  
  # convert to lowercase    
  doc <- tm_map(doc, content_transformer(tolower))
  print("Lowered!")
  
  # remove numbers
  doc <- tm_map(doc, content_transformer(removeNumbers))
  print("Numbers removed!")
  
  # delete english stopwords. See list: stopwords("english")
  doc <- tm_map(doc, removeWords, stopwords("english"))
  print("Stopwords removed!")
  
  # remove punctuations
  doc <- tm_map(doc, content_transformer(removePunctuation))
  print("Punctuations removed!")
  
  # delete common word endings, like -s, -ed, -ing, etc.
  doc <- tm_map(doc, stemDocument, language = "english")
  print("Words stemed!")
  
  # delete multi-whitespace 
  doc <- tm_map(doc, content_transformer(stripWhitespace))  # negation 
  print("Whitespace removed!")
  
  # convert to Document Term Marix
  dtm <- DocumentTermMatrix(doc)
  print("DTM converted!")
  
  # remove sparse-terms   
  dtm.nonsparse <- removeSparseTerms(dtm, 0.95)
  print("Non-sparse DTM generated!")
  
  # stop the timer and print out processing time 
  print(proc.time() - ptm)
  
  # get dtm dataframe
  dat.dtm <- cbind(business_id = input_data$business_id,
                   review_id = input_data$review_id, # primary key
                   review_date = input_data$date, 
                   review_stars = input_data$stars,
                   review_type = input_data$type,
                   user_id = input_data$user_id,
                   as.data.frame(as.matrix(dtm.nonsparse)))
  
  # fill out return list  
  return_list[["doc"]] <- doc
  return_list[["dtm"]] <- dtm
  return_list[["dtm_nonsparse"]] <- dtm.nonsparse
  return_list[["dtm_dataframe"]] <- dat.dtm
  
  return(return_list)
}

# Text Mining #############################################################################
# get partitioned-data
dat_list <- c()
for(i in 1:163) {
  # create a data list 
  dat_list[[i]] <- dat.review[(1+((i-1)*10000)):(i*10000),]
  # status check
  print(paste0("The ", i, "th data partition generated!"))
}

# get the last partition 
dat_list[[164]] <- dat.review[1630001:1630712,]
length(dat_list)

# loop DTMs generator for 164 partitions 
# set up a timer 
ptm <- proc.time()

# doc_list <- c()
# dtm_list <- c()
# dtmdf_list <- c()
# dat.dtm.overall <- data.frame()
# i <- 1

# for (data in dat_list[1:164]) {
#   return.temp <- GetPartitionedDTM(data)
#   doc_list[[i]] <- return.temp$doc
#   dtm_list[[i]] <- return.temp$dtm_nonsparse
#   dtmdf_list[[i]] <- return.temp$dtm_dataframe
#   
#   dat.dtm.overall <- bind_rows(dat.dtm.overall, return.temp$dtm_dataframe)
#   print(paste0("The ", i, "th partitioned data text-mining process done!"))
#   i <- i + 1
# }
# stop the timer
print(proc.time() - ptm)

# Note*: running time/processing time gets longer and longer since doc_list, 
# dtm_list and dtmdf_list gets larger and larger 


# remove columns contain NAs
datclean.dtm.overall <- dat.dtm.overall[,!apply(is.na(dat.dtm.overall), 2, any)]
# transform to JSON file 
datclean.dtm.overall.json <- toJSON(datclean.dtm.overall)
# save to local 
write(datclean.dtm.overall.json, 
      "/Users/yanjin1993/GitHub/Personalize-Yelp-Business/data/datclean_dtm_overall.json")

# overall DTM terms frequency 
dat.overall.freq <- datclean.dtm.overall %>%
  gather(word, frequency, -c(business_id, review_id, review_date, 
                             review_stars, review_type, user_id)) %>%
  filter(frequency != 0) %>%
  group_by(word) %>%    
  summarise(num = n()) %>%            
  arrange(desc(num))

# DTM terms frequency by business ID 
dat.overall.businessid.freq <- datclean.dtm.overall %>%
  gather(word, frequency, -c(business_id, review_id, review_date, 
                             review_stars, review_type, user_id)) %>%
  filter(frequency != 0) %>%
  group_by(word, business_id) %>%    
  summarise(num = n()) %>%            
  arrange(desc(num)) %>%
  spread(word, num) 
# change NA cell values into Os
dat.overall.businessid.freq[is.na(dat.overall.businessid.freq)] <- 0  

# top three words by business_id
dat.businessid.top.freq <- dat.overall.businessid.freq %>% 
  arrange(desc(business_id)) %>%
  group_by(business_id) %>%
  top_n(n = 3) 

# transform to JSON file 
dat.overall.businessid.freq.json <- toJSON(dat.overall.businessid.freq)
dat.businessid.top.freq.json <- toJSON(dat.businessid.top.freq)
# save to local path in JSON form
write(dat.overall.businessid.freq.json, 
      "/Users/yanjin1993/GitHub/Personalize-Yelp-Business/data/dat_overall_businessid_freq.json")
write(dat.businessid.top.freq.json, 
      "/Users/yanjin1993/GitHub/Personalize-Yelp-Business/data/dat_businessid_top_freq.json")

# save to local path in RDS form 
saveRDS(dat.overall.businessid.freq, 
        "/Users/yanjin1993/GitHub/Personalize-Yelp-Business/data/dat_overall_businessid_freq.rds")
saveRDS(dat.businessid.top.freq, 
        "/Users/yanjin1993/GitHub/Personalize-Yelp-Business/data/dat_businessid_top_freq.rds")


# N-gram Text Processing ######################################################################
# get partitioned-data n-gram files 
GetNgramDf <- function(doc, n, sparsity) {
  # DESCRITPION: Return n-gram dataframes 
  # RETURN VALUES: dataframe
  return.list <- list()
  
  NGramTokenizer <- function(x) 
    unlist(lapply(ngrams(words(x), n), paste, collapse = " "), use.names = FALSE)
  
  ngram.dtm <- DocumentTermMatrix(doc, control = list(tokenize = NGramTokenizer))
  ngram.dtm.nonsparse <- removeSparseTerms(ngram.dtm, 1.00 - sparsity)
  ngram.dtm.df <- cbind(Date = dat.combine$Date, 
                        Label = dat.combine$Label,
                        text_body = dat.combine$text_body,
                        as.data.frame(as.matrix(ngram.dtm.nonsparse)))
  return.list[["dtm"]] <- ngram.dtm
  return.list[["dtm.df"]] <- ngram.dtm.df
  return(return.list)
}






# dat.dtm.overall <- datclean.dtm.overall

# combine two partitioned datasets 
# datclean.dtm.overall <- bind_rows(dat.dtm.overall, dat.dtm.overall_1)

