
# coding: utf-8

# In[1]:

get_ipython().magic(u'pylab inline')


# In[2]:

# import easel


# In[2]:

import pandas as pd


# In[3]:

from scipy.cluster.hierarchy import *


# In[4]:

import seaborn as sns


# In[55]:

sns.set_context("talk")
sns.set(font_scale=1.5)


# In[6]:

business_file = '/Users/chalpert/Documents/Columbia/Big_Data/Project/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json'
business_category_file = '/Users/chalpert/Documents/Columbia/Big_Data/Project/yelp_dataset_challenge_academic_dataset/business_category.csv'

business_df = pd.read_json(business_file, orient='records', lines=True)
business_category_df = pd.read_csv(business_category_file)


# In[7]:

restaurant_ids = business_category_df[lambda x: x['categories_value'] == 'Restaurants']['business_id']
restaurant_ids = restaurant_ids.reset_index(drop=True)


# In[8]:

restaurants_df = business_df[lambda x: x['business_id'].isin(restaurant_ids.values)]


# # By category

# In[9]:

business_category_df.head()


# ### Most restaurants have multiple categories
# - Count of categories per restaurant

# In[10]:

(restaurants_df
 .merge(business_category_df, on='business_id')
 .groupby('business_id')
 .categories_value
 .nunique()
 .value_counts()
)


# # Restaurant overlap

# In[11]:

category_count = (restaurants_df
 .merge(business_category_df, on='business_id')
 .groupby('categories_value')
 .business_id
 .nunique()
 .sort_values(ascending=True)
)


# In[12]:

top_categories = category_count[category_count > 100].index


# In[13]:

len(top_categories)


# In[14]:

restaurant_categories = restaurants_df.merge(business_category_df, on='business_id')[['business_id', 'categories_value']]
restaurant_categories['ones'] = 1
restaurant_overlap = pd.pivot_table(restaurant_categories, index='categories_value', columns='business_id', aggfunc=max, values='ones')


# In[15]:

restaurant_overlap = restaurant_overlap.fillna(0)


# In[16]:

def calc_overlap(df):
    jaccard_matrix = np.zeros((len(df),len(df)))
    for i, indx_i in enumerate(df.index):
        for j, indx_j in enumerate(df.index):
            num = (df.ix[[indx_i, indx_j]].sum(axis=0) == 2).sum()
            denom = df.ix[indx_i].sum()
            jaccard_matrix[i, j] = num * 1.0/ denom
    #jaccard_matrix = jaccard_matrix + jaccard_matrix.T
    for i, j in zip(range(len(df)), range(len(df))):
        jaccard_matrix[i, j] = 1
    return jaccard_matrix
            


# In[17]:

overlap =  calc_overlap(restaurant_overlap.reindex(top_categories))


# In[18]:

ctab = pd.DataFrame(overlap, index=restaurant_overlap.reindex(top_categories).index, columns=restaurant_overlap.reindex(top_categories).index)
#ctab = pd.DataFrame(np.corrcoef(goals_by_website), index=goals_by_website.index, columns=goals_by_website.index)
z = linkage(ctab)
d = dendrogram(z, labels=ctab.index,leaf_rotation=90)
sns.despine()


# In[56]:

order = ctab.index[d['leaves']]
ctab_o = ctab.reindex_axis(order, axis=0).reindex_axis(order, axis=1)

mask = np.zeros_like(ctab_o)
#mask[np.triu_indices_from(mask)] = True
with sns.axes_style("white"):
    f, ax = plt.subplots(figsize=(15, 15))
    ax = sns.heatmap(ctab_o, annot=False, linewidths=.5, mask=mask, square=True, cbar=False, fmt='.2f')
    f.suptitle('% of restaurants in row that also have category in column')
ax.set_xlabel('Category')
ax.set_ylabel('Category')
sns.despine()


# In[84]:

ctab_o.sum(axis=1).sort_values()


# In[97]:

ctab_o.ix[['Bagels', 'Indian', 'Japanese'], 'Sushi Bars']


# In[94]:

order = ctab.index[d['leaves']]
ctab_o = ctab.reindex_axis(order, axis=0).reindex_axis(order, axis=1)

mask = np.zeros_like(ctab_o)
#mask[np.triu_indices_from(mask)] = True
sns.set(font_scale=1.5)
with sns.axes_style("white"):
    f, ax = plt.subplots(figsize=(13, 7))
    ax = sns.heatmap(ctab_o.ix[['Bagels', 'Indian', 'Japanese'], :], annot=False, linewidths=.5, square=False, cbar=False, fmt='.2f')
#     f.suptitle('% of restaurants in row that also have category in column')
ax.set_xlabel('Category')
ax.set_ylabel('Category')
sns.despine()
plt.tight_layout()


# ## Restaurant count
# - Top 10

# In[20]:

(restaurants_df
 .merge(business_category_df, on='business_id')
.loc[lambda x: x['categories_value'].isin(top_categories)]
 .groupby('categories_value')
 .business_id
 .nunique()
 .sort_values(ascending=True)
 .iloc[:-1]
).plot(kind='barh')


# ## Avg rating

# In[58]:

sns.set_style('white')


# In[59]:

(restaurants_df
 .merge(business_category_df, on='business_id')
 .loc[lambda x: x['categories_value'].isin(top_categories)]
 .groupby('categories_value')
 .stars
 .mean()
 .sort_values(ascending=True)
 .iloc[-11:]
).plot(kind='barh', xlim=[0,5])
sns.despine()


# In[60]:

(restaurants_df
 .merge(business_category_df, on='business_id')
 .loc[lambda x: x['categories_value'].isin(top_categories)]
 .groupby('categories_value')
 .stars
 .mean()
 .sort_values(ascending=True)
 .iloc[:10]
).plot(kind='barh',  xlim=[0,5])
sns.despine()


# In[140]:

(restaurants_df
 .merge(business_category_df, on='business_id')
 .loc[lambda x: x['categories_value'].isin(top_categories)]
 .groupby('categories_value')
 .stars
 .mean()
 .sort_values(ascending=True)
).plot(kind='barh')


# ## Rating count

# In[150]:

restaurant_count = (restaurants_df
 .merge(business_category_df, on='business_id')
 .loc[lambda x: x['categories_value'].isin(top_categories)]
 .groupby('categories_value')
 .business_id
 .nunique()
 .sort_values(ascending=True))


# In[156]:

rating_count = (restaurants_df
 .merge(business_category_df, on='business_id')
 .loc[lambda x: x['categories_value'].isin(top_categories)]
 .groupby('categories_value')
 .review_count
 .sum()
 .sort_values(ascending=True)
)


# # Rating count per restaurant

# In[261]:

((rating_count / restaurant_count)
.sort_values(ascending=False)
.plot(kind='barh')
)


# # Are higher ratings associated with more checkins?

# # Need to control for restaurant age

# In[29]:

checkin_file = '/Users/chalpert/Documents/Columbia/Big_Data/Project/yelp_dataset_challenge_academic_dataset/Restaurants/checkin.json'

checkin_df = pd.read_json(checkin_file, orient='records')


# In[30]:

checkin_df['total_checkins'] = checkin_df['checkin_info'].apply(lambda x: sum(x.values()))


# In[31]:

checkin_df


# In[ ]:




# In[32]:

categories = ['French']
categories = top_categories


# In[33]:

checkin_count = (restaurants_df
 .merge(business_category_df, on='business_id')
 .merge(checkin_df, on='business_id', how='left')
 .loc[lambda x: x['categories_value'].isin(categories)]
 .groupby('business_id')
 .total_checkins
 .sum()
 .sort_values(ascending=True))


# In[34]:

avg_rating = (restaurants_df
 .merge(business_category_df, on='business_id')
 .loc[lambda x: x['categories_value'].isin(categories)]
 .groupby('business_id')
 .stars
 .mean()
 .sort_values(ascending=True)
)


# In[35]:

checkin_rating = pd.concat([(checkin_count), avg_rating], axis=1)


# In[36]:

checkin_rating.columns = ['avg_checkins', 'avg_rating']


# In[65]:

sns.set(font_scale=2)
sns.set_style('white')
checkin_rating.groupby('avg_rating')['avg_checkins'].mean().plot(xlim=[0, 5.5])
sns.despine()


# In[66]:

sns.set(font_scale=2)
sns.set_style('white')
checkin_rating.plot(x='avg_rating', y='avg_checkins', kind='scatter', ylim=[0, 60000], xlim=[0, 5.5])
sns.despine()


# ## Rating overlap

# # By location

# In[282]:

top_cities = restaurants_df['city'].value_counts().iloc[:30].index


# In[283]:

(restaurants_df
 .merge(business_category_df, on='business_id')
 .loc[lambda x: x['city'].isin(top_cities)]
 .groupby('city')
 .stars
 .mean()
 .sort_values(ascending=True)
).plot(kind='barh')


# # By category-location

# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

reviews = pd.read_json('./reviews.json', orient='records')


# In[206]:

reviews['funny']= reviews['votes'].apply(lambda x: x['funny'])


# In[207]:

reviews['useful']= reviews['votes'].apply(lambda x: x['useful'])


# In[208]:

reviews['cool']= reviews['votes'].apply(lambda x: x['cool'])


# In[4]:

business = pd.read_json('./business.json', orient='records')


# In[8]:

business['attributes'].head()


# In[222]:

reviews.sort_values('funny', ascending=False).head(30).merge(business[['business_id', 'name', 'city']], on='business_id')


# In[ ]:




# In[6]:

review_dist = reviews.groupby('business_id')['user_id'].count()


# In[27]:

review_dist.quantile(.85)


# In[30]:

review_dist.head()


# In[77]:

many_reviews = review_dist[(review_dist > 500)]


# In[195]:

len(many_reviews)


# In[76]:

for i, r in many_reviews.iterkv():
    print i


# In[84]:




# In[102]:

bus[bus['is_good'] != 1.0 ]


# In[131]:

bus.iloc[:100, 'stars']


# In[204]:

ch = easel.Chart()

results = []
for i, r in many_reviews.head(5).iterkv():
    bus = reviews[reviews['business_id'] == i]
    bus = bus.reset_index(drop=True)
    
    
    bus['max'] = 5.0
    ch.line(bus.index, bus['stars'].rolling(7).mean(), alpha=.9)
    
    
ch.set_xlim(0, 500)


# In[152]:

ch = easel.Chart()

results = []
for i, r in many_reviews.iterkv():
    bus = reviews[reviews['business_id'] == i]
    bus = bus.reset_index(drop=True)
    
    
#     bus = pd.DataFrame(bus.groupby('date')['stars'].mean())
    bus['max'] = 5.
    
    review_100 = bus['stars'].iloc[0:100].mean()
    review_200 = bus['stars'].iloc[100:200].mean()
    review_500 = bus['stars'].iloc[500:600].mean()
    results.append({'100': review_100,
                   '200': review_200,
                   '500': review_500,
                   'bus': i})
#     ch.line(bus.index, bus['stars'].rolling(100).sum() / bus['max'].rolling(100).sum(), alpha=.9)
    
    
# ch.set_xlim(0, 500)


# In[165]:

diff = pd.DataFrame(results)


# In[173]:

diff['100'].hist(bins=20)
diff['500'].hist(bins=20, alpha=.5)


# In[190]:

diff['better'] = diff['200'] > diff['100'] + 2* diff.std()['100'] / 10


# In[191]:

diff['worse'] = diff['200'] < diff['100'] - 2* diff.std()['100'] / 10


# In[192]:

diff['diff'] = diff['200'] - diff['100']


# In[193]:

diff[diff['better'] == True]['diff'].hist(bins=20)
diff[diff['worse'] == True]['diff'].hist(bins=30)


# In[189]:




# In[156]:

diff.std()


# In[155]:

diff.mean()


# In[153]:

diff['500'].hist(bins=20)


# In[147]:

(diff['500'] - diff['100']).hist(bins=50)

