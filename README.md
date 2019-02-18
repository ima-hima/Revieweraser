# Insight-DE-2019A-Project


*Reviewer*aser
=============

[![BSD3 license](https://img.shields.io/badge/license-BSD3-blue.svg)](https://github.com/ima-hima/Insight-DE-2019A-Project/blob/master/LICENSE)


Python code to collect data on Amazon reviewers in an attempt to determine which reviewers write
consistently bad reviews. Through a Chrome extension a shopper can select some criteria with which
to judge poor reviewers. Current criteria are:
1. Reviewer consistently gives low reviews.
1. Reviewer consistently gives high reviews.
3. Reviewer consistently writes extemely short reviews.

**Project status:** 1.0 release

| Directory                   | Description of Contents
|:--------------------------- |:---------------------------------------- |
| `src`                       | main code base                           |
| `src/Review-hide_extension` | client-side code for Chrome extension    |
| `src/spark`                 | module that runs spark specifically      |
| `src/wsgi script`           | wsgi server-side code for Apache         |
| `run.sh`                    | shell script to run source               |
| `test`                      | profling and prototyping code            |


See READMEs in each src directory for module-specific notes.
