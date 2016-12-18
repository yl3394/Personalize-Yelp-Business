import sys
sys.path.insert(0, '/var/www/html/flaskapp')
sys.path.insert(1, '/usr/local/spark-2.0.2-bin-hadoop2.7/python')
sys.path.insert(2, '/usr/local/spark-2.0.2-bin-hadoop2.7/python/lib/py4j-0.10.3-src.zip')

import os
os.environ['SPARK_HOME'] = '/usr/local/spark-2.0.2-bin-hadoop2.7'

from flaskapp import app as application
