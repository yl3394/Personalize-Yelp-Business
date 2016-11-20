
# In terminal change path to: 
# cd /Users/yanjin1993/caffe/
# python

# STEP 0 Basic Setups ##########################################################################################
# 0.1 Initiate libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os # for local path 
import caffe  # for feature extraction 
from glob import glob 
from os import path
import sys

caffe_root='/Users/yanjin1993/caffe/'
sys.path.insert(0, caffe_root + 'python')

# An instance of RcParams for handling default matplotlib values ??????????
plt.rcParams['figure.figsize']=(10,10)
plt.rcParams['image.interpolation'] = 'nearest'
plt.rcParams['image.cmap'] = 'gray'


# STEP 1 DOWNLOAD REFERENCE MODEL (CaffeNet) ######################################################################

# Test existence of reference model in your local path 
#if os.path.isfile(caffe_root + 'models/bvlc_reference_caffenet/bvlc_reference_caffenet.caffemodel'):
#    print 'CaffeNet found.'
#else:
#    print 'Need to download pre-trained CaffeNet model...'

# If not in terminal 
# /Users/yanjin1993/caffe/scripts/download_model_binary.py /Users/yanjin1993/caffe/models/bvlc_reference_caffenet



# STEP 2 LOAD NET AND BASIC SET UP ###############################################################################
# 2.1 Set Caffe to CPU mode and load the net from disk 
caffe.set_mode_cpu()

net = caffe.Net(caffe_root + 'models/bvlc_reference_caffenet/deploy.prototxt',
                caffe_root + 'models/bvlc_reference_caffenet/bvlc_reference_caffenet.caffemodel',
                caffe.TEST) 

# 2.2 Set up input processing 
# Customizable code here, we use caffe.io.Transformer here 
# load the mean ImageNet image (as distributed with Caffe) for subtraction
mu = np.load(caffe_root + 'python/caffe/imagenet/ilsvrc_2012_mean.npy')
mu = mu.mean(1).mean(1)  # average over pixels to obtain the mean (BGR) pixel values
print 'mean-subtracted values:', zip('BGR', mu)

# 2.3 Create transformer for the input called 'data'
transformer = caffe.io.Transformer({'data': net.blobs['data'].data.shape})

transformer.set_transpose('data', (2,0,1))  # move image channels to outermost dimension
transformer.set_mean('data', mu)            # subtract the dataset-mean value in each channel
transformer.set_raw_scale('data', 255)      # rescale from [0, 1] to [0, 255]
transformer.set_channel_swap('data', (2,1,0))  # swap channels from RGB to BGR

# 2.4 Set the size of the input 
net.blobs['data'].reshape(1,        # batch size
                          3,         # 3-channel (BGR) images
                          227, 227)  # image size is 227x227


# STEP 3. IMAGE DIRECTORY ################################################################################
# R code 
#############

# STEP 4. FEATURE EXTRACTION ################################################################################
# Norm2 Layer ----------------------------------------------------------------------
name = pd.read_csv('/Users/yanjin1993/Desktop/dat_test.csv')
X_train_name = name['img_directory']

i = 0
image = caffe.io.load_image(X_train_name[0])
net.blobs['data'].data[...] = transformer.preprocess('data', image)
net.forward()
feature = np.reshape(net.blobs['norm2'].data[0], 43264, order='C')

for name in X_train_name[1:]:
    image = caffe.io.load_image(name)
    net.blobs['data'].data[...] = transformer.preprocess('data', image)
    net.forward()
    feature = np.vstack([feature,np.reshape(net.blobs['norm2'].data[0], 43264, order='C')])
    i += 1
    print i

print feature.shape, X_train_name.shape
data_norm2 = np.column_stack([X_train_name.T,feature])
data_norm2 = pd.DataFrame(data_norm2)
data_norm2.to_csv('/Users/yanjin1993/Desktop/data_norm2.csv')


# Row 1 ---------------------------------------------------------------------------
# If row 1 does not work THEN RUN THIS!!!! 
image001 = caffe.io.load_image(X_train_name[0])
net.blobs['data'].data[...] = transformer.preprocess('data', image001)
net.forward()
featurerow1 = np.reshape(net.blobs['norm2'].data[0], 43264, order='C')
# Change into array list 
featurerow1 = np.vstack(featurerow1)
print featurerow1.shape, X_train_name.shape
data_n2_row1 = np.column_stack([featurerow1])
data_n2_row1 = pd.DataFrame(data_n2_row1)
data_n2_row1.to_csv('/Users/yanjin1993/Desktop/data_n2_row1.csv')



#-----------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------
# FC6 Layer ------------------------------------------------------------------------
#name = pd.read_csv('/Users/yanjin1993/Google Drive/Columbia University /2016 Fall /Applied Data Science /Project_003/image_classification/data /exported_data/dat_namelist.csv')
#X_train_name = name['img_directory']

#i = 0
#image = caffe.io.load_image(X_train_name[0])
#net.blobs['data'].data[...] = transformer.preprocess('data', image)
#net.forward()
#feature = np.array(net.blobs['fc6'].data[0])
#for name in X_train_name[1:]:
#    image = caffe.io.load_image(name)
#    net.blobs['data'].data[...] = transformer.preprocess('data', image)
#    net.forward()
#    feature = np.vstack([feature,(np.array(net.blobs['fc6'].data[0]))])
#    i += 1
#    print i


# Save to local 
#print feature.shape, X_train_name.shape
#data_fc6 = np.column_stack([X_train_name.T,feature])
#data_fc6 = pd.DataFrame(data_fc6)
#data_fc6.to_csv('/Users/yanjin1993/Google Drive/Columbia University /2016 Fall /Applied Data Science /Project_003/image_classification/data /exported_data/data_fc6.csv')

#print feature.shape, X_train_name.shape


# STEP 5. LAYER VISUALIZATION ################################################################################
image = caffe.io.load_image(caffe_root + 'examples/images/chicken_0046.jpg')
plt.imshow(image)

# For Conv 1 Layer  -------------------------------------------------------
# copy the image data into the memory allocated for the net
net.blobs['data'].data[...] = transformer.preprocess('data', image)
net.forward()
feature = np.array(net.blobs['conv1'].data[0])

# Visualization function: 
def vis_square(data):
    """Take an array of shape (n, height, width) or (n, height, width, 3)
       and visualize each (height, width) thing in a grid of size approx. sqrt(n) by sqrt(n)"""
    
    # normalize data for display
    data = (data - data.min()) / (data.max() - data.min())
    
    # force the number of filters to be square
    n = int(np.ceil(np.sqrt(data.shape[0])))
    padding = (((0, n ** 2 - data.shape[0]),
               (0, 1), (0, 1))                 # add some space between filters
               + ((0, 0),) * (data.ndim - 3))  # don't pad the last dimension (if there is one)
    data = np.pad(data, padding, mode='constant', constant_values=1)  # pad with ones (white)
    
    # tile the filters into an image
    data = data.reshape((n, n) + data.shape[1:]).transpose((0, 2, 1, 3) + tuple(range(4, data.ndim + 1)))
    data = data.reshape((n * data.shape[1], n * data.shape[3]) + data.shape[4:])
    
    plt.imshow(data); plt.axis('off')



net.blobs['fc6'].data[0].shape
# Visualize the layer conv1 
filters = net.params['conv1'][0].data
vis_square(filters.transpose(0, 2, 3, 1))

a = np.reshape(net.blobs['conv1'].data[0], 290400, order='C')
a.max()

# For Norm 1 Layer ------------------------------------------------------------
feat = net.blobs['norm1'].data[0]
# visualize the layer norm1 
vis_square(feat)

# Show the images (When runnning in terminal)
import pylab
pylab.show()



















