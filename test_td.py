from __future__ import print_function

import os
import pdb
import json

from flask import Flask, jsonify

from sqlalchemy_teradata import VARCHAR as Varchar

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TIMESTAMP as Timestamp
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

def parse_data(res):
    data = []
    for d in res:
        x = dict(d)
        x['CREATED_AT'] = \
            x['CREATED_AT'].replace(tzinfo=None).strftime('%Y/%m/%d')
        x['UPDATED_AT'] = \
            x['UPDATED_AT'].replace(tzinfo=None).strftime('%Y/%m/%d')
        x['ID'] = int(x['ID'])
        data.append(x)
    return data

@app.route('/', methods=['Get'])
def hello():
    td_engine = create_engine(cred)
    sel = chronos_stream_type.select()

    with td_engine.connect() as conn:
        res = conn.execute(sel)

    data = parse_data(res)
    return jsonify(data)
    # return "Hello World" 

if __name__ == '__main__':
    # port = int(os.getenv('PORT', 9099))
    # app.run(host='0.0.0.0', port=port)
    port = 8888
    app.run(host='0.0.0.0', port=port)

