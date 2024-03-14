import pandas as pd
from datetime import date, timedelta, datetime
from df_extract import extraction
from sqlalchemy import create_engine, URL
from sqlalchemy.types import *
from pprint import pprint
from tqdm import tqdm
from pandas._testing import rands_array
from df_load import load

yesterday = date.today() - timedelta(days=1)
print(yesterday)

start_date = yesterday
end_date = yesterday

delta = end_date - start_date

for i in tqdm(range(delta.days + 1)):

    # Sumamos un dia por cada ocasion
    day = start_date + timedelta(days=i)
    day = datetime.strptime(str(day), "%Y-%m-%d").strftime("%Y/%m/%d")
    print(f'Subiendo dia {day}: \n')
    # ZONAS DE CARGA BCA
    # lista_ZC = 'TIJUANA,ENSENADA,MEXICALI,SANLUIS'
    # ZONAS DE CARGA BCS
    lista_ZC = 'CONSTITUCION,LA-PAZ,LOS-CABOS'

    # Extraemos los datos
    responseJSON = extraction(lista_ZC, day, 'BCS', 'MDA')
    # DATAFRAME
    final_df = pd.DataFrame()
    # Iteramos por ZONA_CARGA
    for j in range(len(responseJSON['Resultados'])):
        # ZONA DE CARGA
        fecha = []
        horas = []
        pz = []
        pz_ene = []
        pz_per = []
        pz_cng = []

        zc = responseJSON['Resultados'][j]['zona_carga']

        # Iteramos por cada Hora
        for i in range(len(responseJSON['Resultados'][j]['Valores'])):
            # FECHAS
            fecha.append(responseJSON['Resultados'][j]['Valores'][0]['fecha'])
            # HORAS
            horas.append(responseJSON['Resultados'][j]['Valores'][i]['hora'])
            # PZ
            pz.append(responseJSON['Resultados'][j]['Valores'][i]['pz'])
            # PZ_ENE
            pz_ene.append(responseJSON['Resultados'][j]['Valores'][i]['pz_ene'])
            # PZ_PER
            pz_per.append(responseJSON['Resultados'][j]['Valores'][i]['pz_per'])
            # PZ_CNG
            pz_cng.append(responseJSON['Resultados'][j]['Valores'][i]['pz_cng'])

        # Es guardado todo en un DataFrame agregando el sistema propio
        dict_PEND = {'sistema':'BCS', 'zona_carga':zc,  'fecha':fecha,  'hora':horas,  'pz':pz, 'pz_ene':pz_ene, 'pz_per':pz_per, 'pz_cng':pz_cng}
        df = pd.DataFrame(dict_PEND)

        final_df = pd.concat([final_df, df], ignore_index=True)
        

    '''UNIQUE'''
    N = len(final_df.index)
    s_arr = pd._testing.rands_array(40, N)
    final_df['unique'] = s_arr

    '''id user'''
    final_df['id_user'] = 1

    '''created'''
    timed = pd.Timestamp.now()
    final_df['created'] = timed.strftime("%Y-%m-%d %H:%M:%S")

    '''estatus'''
    final_df['estatus'] = 1

    final_df = final_df.reindex(columns = ['unique', 'sistema', 'zona_carga', 'fecha', 'hora', 'pz', 'pz_ene', 'pz_per', 'pz_cng', 'id_user', 'created', 'estatus'])
    #print(final_df.tail(30))

    # LISTO PARA SUBIRSE EL DIA A LA BD
    load(final_df)