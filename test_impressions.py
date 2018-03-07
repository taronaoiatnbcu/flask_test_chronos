from __future__ import print_function

import os
import pdb
import json

from flask import Flask, jsonify

from sqlalchemy_teradata import VARCHAR as Varchar

from sqlalchemy import create_engine, Table, Column, Integer, String,\
                       MetaData, Boolean, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TIMESTAMP as Timestamp
from sqlalchemy.types import DECIMAL, TIME, Date
from sqlalchemy.sql import select

meta = MetaData()
Base = declarative_base()

app = Flask(__name__, static_url_path = "")

user = 'CHRONOS_BI_01'
pasw = 'Nbcu@1234'
host = 'tddev.stg-tfayd.com'
cred = 'teradata://%s:%s@%s' % (user, pasw, host)

chronos_stream_type = Table(
    'CHRONO_PG_STREAMTYPE',
    meta,
    Column('ID', Integer, primary_key=True),
    Column('STREAMTYPE_NAME', Varchar(3000)),
    Column('DESCRIPTION', Varchar(3000)),
    Column('CREATED_BY', Varchar(255)),
    Column('UPDATED_BY', Varchar(255)),
    Column('CREATED_AT', Timestamp(timezone=True)),
    Column('UPDATED_AT', Timestamp(timezone=True)),
    schema = 'RESEARCH_VMBI'
)

impressions = Table(
    'CHRONO_PG_IMPRESSIONS',
    meta,
    Column('ID', Integer, primary_key=True),
    Column('TMS_ID', Integer),
    Column('PROGRAM', Varchar(255)),

    Column('PROGRAM_ID', Integer),
    Column('STREAM_ID', Integer),

    Column('STREAM_TYPE', Varchar(255)),
    Column('NETWORK', Varchar(255)),
    Column('DEMO', Varchar(255)),
    Column('DATA_SOURCE', Varchar(255)),

    Column('IMPRESSIONS', Integer),
    Column('RATINGS', DECIMAL),
    Column('CONTEXT', Integer),

    Column('IMPRESSIONS_TIME', Time),
    Column('IMPRESSIONS_DATE', Date),
    Column('SECOND_OF_PROGRAM', Integer),

    Column('CREATED_BY', Varchar(255)),
    Column('UPDATED_BY', Varchar(255)),

    Column('CREATED_AT', Timestamp(timezone=True)),
    Column('UPDATED_AT', Timestamp(timezone=True)),

    Column('SPECIAL', Varchar(6)),
    Column('RETITLE', Varchar(6)),
    Column('DAY_OF_WEEK', Integer),

    schema = 'RESEARCH_VMBI'
)


non_content = Table(
    'CHRONO_PG_NON_CONTENT',
    meta,
    Column('ID', Integer, primary_key=True),
    Column('TMS_ID', Integer),
    Column('PROGRAM_ID', Integer),
    Column('IS_COMMERCIAL', Integer),
    Column('NON_CONTENT_DATE', Timestamp),
    Column('NETWORK', Varchar(3000)),
    Column('TITLE_NM', Varchar(3000)),
    Column('START_SECOND_OF_PROGRAM', Integer),
    Column('OTHER_NON_CONTENT_DURATION_SECONDS', Integer),
    Column('COMMERCIAL_DURATION_SECONDS', Integer),
    Column('DATA_SOURCE', Varchar(3000)),
    Column('CREATED_BY', Varchar(255)),
    Column('UPDATED_BY', Varchar(255)),
    Column('CREATED_AT', Timestamp(timezone=True)),
    Column('UPDATED_AT', Timestamp(timezone=True)),
    schema = 'RESEARCH_VMBI'
)

# def parse_data(res):
    # data = []
    # for d in res:
        # x = dict(d)
        # x['ID'] = int(x['ID'])
        # x['PROGRAM_ID'] = int(x['PROGRAM_ID'])
        # x['START_SECOND_OF_PROGRAM'] = int(x['START_SECOND_OF_PROGRAM'])
        # x['OTHER_NON_CONTENT_DURATION_SECONDS'] = int(x['OTHER_NON_CONTENT_DURATION_SECONDS'])
        # x['NON_CONTENT_DATE'] = x['NON_CONTENT_DATE'].replace().strftime('%Y/%m/%d')
        # x['COMMERCIAL_DURATION_SECONDS'] = int(x['COMMERCIAL_DURATION_SECONDS'])
        # x['CREATED_AT'] = x['CREATED_AT'].replace(tzinfo=None).strftime('%Y/%m/%d')
        # x['UPDATED_AT'] = x['UPDATED_AT'].replace(tzinfo=None).strftime('%Y/%m/%d')
        # data.append(x)
    # return data

def parse_data(res):
    data = []

    for d in res:
        x = dict(d)
        y = {}

        y['impressions'] = int(x['IMPRESSIONS'])
        y['ratings'] = float(x['RATINGS'])
        y['second_of_program'] = int(x['SECOND_OF_PROGRAM'])
        y['time'] = x['IMPRESSIONS_TIME'].strftime('%H:%M:%S')
        y['percent_change_in_impressions'] = None 

        data.append(y)

    data = sorted(data, key=lambda k: k['second_of_program'])
    dat = {
         "program_id": "1307",
         "startTime": "20:00:00",
         "streamId": 1,
         "streamType": "L",
         "program": "RIO NBC PRIME",
         "network": "NBC",
         "special": False,
         "retitle": False,
         "date": "8/20/2016",
         "time_series_data_by_source" : {
                    "NIELSEN" : {
                    "A2+" : data
                    }
                }
          }

    return dat

@app.route('/program/1037/context', methods=['Get'])
def hello():
    td_engine = create_engine(cred)
    sel = select([impressions.c.IMPRESSIONS,
                  impressions.c.RATINGS,
                  impressions.c.SECOND_OF_PROGRAM,
                  impressions.c.IMPRESSIONS_TIME])\
        .where(impressions.c.PROGRAM_ID =='15900001')\
        .where(impressions.c.STREAM_ID =='1')\
        .where(impressions.c.DEMO =='A2+')\
        .where(impressions.c.STREAM_TYPE =='LIVE')\
        .where(impressions.c.IMPRESSIONS_DATE =='2018-01-18')

    import time 
    start = time.time()
    with td_engine.connect() as conn:
        res = conn.execute(sel)
    end = time.time()
    print(end - start)
    data = parse_data(res)
    response = jsonify(data) 
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response 


if __name__ == '__main__':
    # port = int(os.getenv('PORT', 9099))
    # app.run(host='0.0.0.0', port=port)
    port = 8888
    app.run(host='0.0.0.0', port=port)
