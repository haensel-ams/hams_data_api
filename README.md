HAMS Analytics Toolkit
===

# Install

```
python setup.py install
```

This installs the package **hams_data_api**.



# Usage

```
from hams_data_api import data_api
```

Have a look at the example jupyter notebook:
**example_usage_hams_data_api.ipynb**


## apis



## data

- **db_mysql**: MySQL Database Handler with query to dataframe
- **db_mongo**: MongoDB Database Handler with query to dataframe
- **aws_athena**: AWS-Athena Handler with SQL-query to dataframe

```
from hams_analytics_package.data import db_mysql
from hams_analytics_package.data import db_mongo
from hams_analytics_package.data import aws_athena
```

## helper

**utils**: collection of some helper functions, mainly attribution usage
```
from hams_analytics_package.helper import utils
```

## plotting

**nothing implemented yet**


# to come
- plotting fucntionalities
- some standardized ML modules
- more API connections
- improved documentation with examples ;-)


## Troubleshooting
If your install fails, make sure you have following packages:
```
sudo apt-get install gcc
sudo apt-get install libpq-dev
sudo apt-get install python-dev
```

It might be that you run into problem with the installation of the newest version of required package: **googleads**. In that case try installing version 14.1.0 using:
```
pip install googleads==14.1.0
```
