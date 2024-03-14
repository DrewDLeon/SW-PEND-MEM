import pandas as pd
from sqlalchemy import create_engine, URL
from sqlalchemy.types import *
from db_params import params

db = params()

print(db.user)

def load(dataframe):
    engine = create_engine(f'mysql+mysqlconnector://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}')

    df_schema = {
        "id_cenace_mda":INTEGER,
        "unique":VARCHAR(40),
        "sistema":VARCHAR(3),
        "zona_carga":VARCHAR(40),
        "fecha":DATE,
        "hora":INT,
        "pz":FLOAT,
        "pz_ene":FLOAT,
        "pz_per":FLOAT,
        "pz_cng":FLOAT,
        "id_user":INT, # # 1 por default
        "created":DATETIME, # NOW TIME
        "modified":INT, # NULL por default
        "deleted":INT, # NULL por default
        "estatus":INT # 1 por default
    }

    dataframe.to_sql(
        'tbl_cenace_mda',
        con = engine,
        if_exists = 'append',
        index = False,
        dtype = df_schema
    )
