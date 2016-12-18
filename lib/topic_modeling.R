# Basic Setup #############################################################################

# We use 5 different starting points (nstart=5) – that is, five independent runs. Each starting point 
# requires a seed integer (this also ensures reproducibility),  so I have provided 5 random integers in 
# my seed list. Finally I’ve set best to TRUE (actually a default setting), which instructs the algorithm 
# to return results of the run with the highest posterior probability.

# As mentioned earlier,  there is an important parameter that must be specified upfront: k, the number of 
# topics that the algorithm should use to classify documents. There are mathematical approaches to this, 
# but they often do not yield semantically meaningful choices of k (see this post on stackoverflow for an example). 


# load package
library(jsonlite)
library(dplyr)
library(tidyr)
library(NLP)
library(tm)
library(topicmodels)

# load DTM created environment 
load("/Users/yanjin1993/GitHub/Personalize-Yelp-Business/tm/environment/100_enviornment_dtm_clean.RData")
# set working directory
setwd("/Users/yanjin1993/GitHub/Personalize-Yelp-Business/")

#Set parameters for Gibbs sampling
burnin <- 4000
iter <- 2000
thin <- 500
seed <-list(2003,5,63,100001,765)
nstart <- 5
best <- TRUE

#Number of topics
k <- 5  # do we have to cv this parameter

# unigram ################################################################################
# change back to DTM form
mx.dtm.complete <- as.matrix(datclean.dtm.overall %>% 
                    select(-c(business_id, review_id, 
                              review_date, review_stars, 
                              review_type, user_id)))

# remove rowsum 0 rows 
mx.dtm.complete <- mx.dtm.complete[rowSums(mx.dtm.complete)!=0, ] 


#Run LDA using Gibbs sampling
model_LDA_5topics <-LDA(mx.dtm.complete[1:200,],k, method="Gibbs", 
                        control=list(nstart=nstart,seed = seed, 
                                     best=best, burnin = burnin, 
                                     iter = iter, thin=thin))

# topic label for each review text line
ldaOut.topics <- cbind(datclean.dtm.overall$business_id[1:100], 
                       as.matrix(topics(model_LDA_5topics)))
# top terms for each topic 
ldaOut.terms <- as.data.frame(terms(model_LDA_5topics,6))

# probability for each topic 
topicProbabilities <- as.data.frame(model_LDA_5topics@gamma)


# bigram ################################################################################
# load RDS 
dat.dtm.gram2 <- readRDS("/Users/yanjin1993/GitHub/Personalize-Yelp-Business/output/dat_dtm_gram2_overall.rds")
# remove columns contain NAs
dat.dtm.gram2[is.na(dat.dtm.gram2)] <- 0 
# make a copy for original file 
datclean.dtm.gram2 <- dat.dtm.gram2
# only keep colSums >= 300 ????? do we need to CV this value 
datclean.dtm.gram2 <- datclean.dtm.gram2[, colSums(datclean.dtm.gram2) >= 300] # reduce dim 800+ -> 444

# remove some common word columns 
datclean.dtm.gram2 <- datclean.dtm.gram2[ , -which(names(datclean.dtm.gram2) 
                             %in% c("food good", "food great",
                                    "go back", "good servic", "great servic", 
                                    "realli good", "great place", "pretti good",
                                    "great food", "seem like", "feel like"))] 
# get bigram DTM form 
mx.dtm.2gram <- as.matrix(datclean.dtm.gram2) # no need to remove business_id related info
# remove rowSums = 0 rows 
mx.dtm.2gram <- mx.dtm.2gram[rowSums(mx.dtm.2gram)!=0, ] 

# get row number for rowsum == 0 rows
temp <- as.matrix(datclean.dtm.gram2)
lst.rowsum.zero <- c(which(rowSums(temp)==0))
# load data with business id and review id 
file_path <- "/Users/yanjin1993/Desktop/Restaurants/reviews.json"
dat.review <- fromJSON(txt=file_path)
# get metd info 
dat.colname <- dat.review[-lst.rowsum.zero, c("business_id", "review_id", "stars")]
rm(temp)

# LDA Topic Modeling #######################################################################
# Run LDA using Gibbs sampling
ptm <- proc.time()
model_LDA_5topics_bigram <-LDA(mx.dtm.2gram, 8, method="Gibbs", 
                        control=list(nstart=nstart,seed = seed, 
                                     best=best, burnin = burnin, 
                                     iter = iter, thin=thin))
print(proc.time() - ptm)

# top terms for each topic --------------------------------------------------
dat.terms.bigram <- as.data.frame(terms(model_LDA_5topics_bigram, 10))

# save to local 
saveRDS(dat.terms.bigram, "output/bigram_terms.rds")
# transform to JSON file 
dat.terms.bigram.json <- toJSON(dat.terms.bigram)
# save to local path in JSON form
write(dat.terms.bigram.json, "output/bigram_terms.json")

# probability for each topic --------------------------------------------------
dat.topicmodel.bigram <- cbind(dat.colname, as.data.frame(topics(model_LDA_5topics_bigram)),
                              as.data.frame(model_LDA_5topics_bigram@gamma)) 

colnames(dat.topicmodel.bigram)[c(4:12)] <- c("topic", "topic_1_prob", "topic_2_prob", 
                                        "topic3_prob", "topic_4_prob", "topic_5_prob", 
                                        "topic_6_prob", "topic_7_prob", "topic_8_prob")

# save to local 
saveRDS(dat.topicmodel.bigram, "output/topicmodel_bigram.rds")
# transform to JSON file 
dat.topicmodel.bigram.json <- toJSON(dat.topicmodel.bigram)
# save to local path in JSON form
write(dat.topicmodel.bigram.json, "output/bigram_topics.json")

# Aggregate to Business ID Level ############################################################



