# *Reviewer*aser

[![BSD3 license](https://img.shields.io/badge/license-BSD3-blue.svg)](https://github.com/ima-hima/Insight-DE-2019A-Project/blob/master/LICENSE)

**Project status:** 1.0 release

Python code to collect data on Amazon reviewers in an attempt to determine which reviewers write
consistently bad reviews. Through a Chrome extension a shopper can select some criteria with which
to judge poor reviewers. Current criteria are:

1. Reviewer consistently gives low reviews.
1. Reviewer consistently gives high reviews.
1. Reviewer consistently writes extemely short reviews.



The project pipeline is:

1. Data comes out of S3 in .gz files.
1. It is processed through Spark.
1. And goes into a Redis database.
1. The Chrome extension does a DOM pass and collects a list of all users on the page.
1. It queries the Apache server, which in turn queries the Redis DB.
1. The extension hides reviews based on settings.

![](https://raw.githubusercontent.com/ima-hima/Revieweraser/master/Pipeline.png)

There are three sets of source:

1. `src/spark/spark_run.py`, which contains all the code for the Spark pipeline: pull from S3, process, push to Redis.
1. `src/wsgi script/wsgy.py`, which runs on a remote server and presents an API to the outside world. It takes in a GET request, queries the Redis server, and returns JSON.
1. `src/Review-hide_extension/`, which is a set of javascript, html and css files for the Chrome extension.

For each of these source files, please see the respective `README` files in the src folders for pseudo code.


| Directory                   | Description of Contents
|:--------------------------- |:---------------------------------------- |
| `src`                       | main code base                           |
| `src/Review-hide_extension` | client-side code for Chrome extension    |
| `src/spark`                 | module that runs spark specifically      |
| `src/wsgi script`           | wsgi server-side code for Apache         |
| `run.sh`                    | shell script to run source               |
| `test`                      | profling and prototyping code            |
