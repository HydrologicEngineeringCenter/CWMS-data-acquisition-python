# cda-post
This simple Python program reads time-series from a text file and posts to the CWMS Data Api (CDA)

Environment Variables

|name| description                        |
|---|------------------------------------|
|CDA_SERVICE | server name hosting the webservice |
|CDA_KEY| key for authorization              |


|example usage| notes |
|---|------|
|python main.py CDAInput.csv | reads CDAInput.csv and saves to web service |
|python main.py CDAInput.csv dry-run| reads CDAInput.csv without saving to the web service |


## How to run the tests

python test.py 


## Development Environment

The code was developed with Python 3.11
