#!/bin/env python3
# This getUSGS script works with CDA version 20250305
# and cwms-python version 0.6

import pandas as pd
import cwms
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-f", "--filename", default="1", help="Days back from current time to check for updated ratings.  Can be decimal and integer values")
parser.add_argument("-a", "--api_root", required=True, type=str, help="Api Root for CDA (Required).")
parser.add_argument("-k", "--api_key", default=None, type=str, help="api key. one of api_key or api_key_loc are required")
parser.add_argument("-kl", "--api_key_loc", default=None, type=str, help="file storing Api Key. One of api_key or api_key_loc are required")
args = vars(parser.parse_args())

# create logger for logging
logger = logging.getLogger()
if (logger.hasHandlers()):
    logger.handlers.clear()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

APIROOT = args["api_root"]
# place a file in .cwms names api_key that holds you apikey
# to be used to write data to the database using CDA
if args["api_key_loc"] is not None:
    api_key_loc = args["api_key_loc"]
    with open(api_key_loc, "r") as f:
        APIKEY = f.readline().strip()
elif args["api_key"] is not None:
    APIKEY=args["api_key"]
else:
    raise Exception("must add a value to either --api_key(-a) or --api_key_loc(-al)") 

# Days back is defined as a argument to the program.
# run program by typing python3 getUSGS.py 5
# the 5 would mean grab data starting 5 days ago to now
INI_FILENAME = args["filename"]

# import CWMS module and assign the apiROOT and apikey to be
# used throughout the program
apiKey = "apikey " + APIKEY
api = cwms.api.init_session(api_root=APIROOT, api_key=apiKey)


rating_types = {'store_corr': 
            {'db_type':'db_corr', 'db_disc':'USGS-CORR'},
            'store_base':
            {'db_type':'db_base', 'db_disc':'USGS-BASE'},
            'store_exsa':
            {'db_type':'db_exsa', 'db_disc':'USGS-EXSA'}}

def parse_ini_line(line) :
    '''
    Parses a line in the ini_file into fields
    '''
    if line.find("'") > 0 or line.find('"') > 0 :
        #---------------------------#
        # fields with spaces quoted #
        #---------------------------#
        c1 = [c for c in line]
        escape = False
        quote = None
        c2 = []
        for c in c1 :
            if c == '\\' :
                escape = not escape
                if not escape : c2.append(c)
                continue
            if c in ('"', "'") :
                if not quote :
                    quote = c
                    continue
                if c == quote :
                    quote = None
                    continue
            if c.isspace() and quote :
                c = chr(0)
            c2.append(c)
        fields = "".join(c2).split()
        for i in range(len(fields)) :
            fields[i] = fields[i].replace(chr(0), " ")
    elif line.find("\t") > 0 :
        #------------------------------------------------------------#
        # all fields without spaces separated by tabs (version < 5.0 #
        #------------------------------------------------------------#
        fields = line.split("\t")
    else :
        #-----------------------#
        # no fields with spaces #
        #-----------------------#
        fields = line.split()
    return fields

def update_rating_spec(rating_id, office_id, db_disc):
    rating_spec = cwms.get_rating_spec(rating_id=rating_id,office_id=office_id)
    data = rating_spec.df
    data = data.drop('effective-dates',axis=1)
    logger.info(f'Setting source-agency to USGS')
    data['source-agency'] = 'USGS'
    logger.info(f'Setting Active, Auto-update, Auto-Activate to True')
    data['active'] = True
    data['auto-update'] = True
    data['auto-activate'] = True
    if 'description' in data.columns:
        if db_disc not in data.loc[0,'description']:
            data['description'] = data['description'] + ' ' + db_disc
    else:
        data['description'] = db_disc
    disc = data.loc[0,'description']
    logger.info(f'Saving specification discription as: {disc}')
    data_xml = cwms.rating_spec_df_to_xml(data)
    cwms.store_rating_spec(data=data_xml, fail_if_exists=False)


def main():
    logger.info(f"CDA connection: {APIROOT}")
    logger.info(f"Opening ini file: {INI_FILENAME}")
    ini_file = open(INI_FILENAME, "r")
    lines = ini_file.readlines()
    ini_file.close()

    params = {}
    keywords = ['cwms_office','db_base','db_exsa','db_corr','localid']
    rating_errors = []
    for i in range(len(lines)) :
        line = lines[i][:-1].strip()
        try :
            line = line[:line.index("#")].strip() # strip comments
        except :
            pass
        if not line : continue
        if '=' in line: 
            fields = line.split('=')
            if fields[0] in keywords:
                params[fields[0]] = fields[1]
        else:
            fields = parse_ini_line(line)
            if fields[0] in rating_types.keys():
                rating_db_type = rating_types[fields[0]]['db_type']
                if f'$(${rating_db_type})' in fields:
                    rating_spec = params[rating_db_type].replace('\$localid', params['localid'])
                    logger.info(f'Updating rating specification: {rating_spec}')
                    try:
                        update_rating_spec(rating_spec, params['cwms_office'], rating_types[fields[0]]['db_disc'])
                        logger.info('SUCCESS: rating specification changes stored')
                    except:
                        logger.error('ERROR: rating specificataion could not be update')
                        rating_errors.append([rating_spec, rating_types[fields[0]]['db_disc']])
    logger.info(f'ERRORS: The following rating specifications could not be updated {rating_errors}')


if __name__ == "__main__":
    main()