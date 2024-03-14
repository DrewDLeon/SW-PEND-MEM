from datetime import timedelta, date, datetime
from pprint import pprint
from dotenv import load_dotenv
import os
import requests

load_dotenv()

# Constantes
url = os.environ.get('URL')
formato = 'JSON'

def extraction(zc, fecha, sistema, proceso):

    # URL
    url_final = url + sistema+'/'+proceso+'/'+zc+'/'+fecha+'/'+fecha+'/'+formato
    # Extract
    response = requests.request('GET', url_final)

    # JSON parse
    responseJSON = response.json()

    return responseJSON
