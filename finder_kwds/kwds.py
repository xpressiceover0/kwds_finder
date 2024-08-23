# -*- coding: utf-8 -*-
# Vectorizer
from email.policy import default
from sklearn.feature_extraction.text import CountVectorizer as CV

# servidor
import uvicorn
from fastapi import FastAPI, Body, Header
from fastapi.middleware.cors import CORSMiddleware
#from pydantic import BaseModel, Field
from typing import List, Optional #, Optional

# pool de hilos para navegadores
from concurrent.futures import ThreadPoolExecutor

# db
import mysql.connector
from mysql.connector import errorcode

# misc
#from urllib import request as urlreq
#import ssl
#import json
#import time
#import datetime
import os
#import subprocess
import re
#import ftplib
#from queue import Queue
#import uuid
#import dotenv
#import random as rnd
#import numpy as np
import pandas as pd

# La clase fastApi inicia los endpionts de la api
app=FastAPI()

app.add_middleware(CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'])
#___________________________


def getConexion(name):
# Abre una conexion con la DB y regresa un conector conn
    data_conn = {
            'user': os.environ['dbuser'],
            'password': os.environ['dbpass'],
            'host': '10.10.10.10',
            'port': '3306',
            'database': name,
            'raise_on_warnings': True}
    try:
        conn = mysql.connector.connect(**data_conn)
        cursor=conn.cursor(buffered=True)
        return (conn, cursor)
    
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Tu username o password son erroneos")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"La base de datos {data_conn['database']} no existe")
        else:
            print(err)
        return (None, None)

#________________________________________________________________________________________________________
def numparser(text):
    pars={
    'unidades':
        ["cero", "uno", "dos", "tres", "cuatro", "cinco","seis", "siete", "ocho", "nueve"],
    'decenas':
        ["diez","veinte","treinta","cuarenta","cincuenta","sesenta","setenta","ochenta","noventa"],
    'centenas':
        ["cien(?:tos?)?"],
    'miles':
        ["mil(?:es)?"],
    'millones':
        ["millon(?:es)?"],
    'billones':
        ["billon(?:es)?"],
    'trillones':
        ["trillon(?:es)?"]
    }
        
    for orden in list(pars.keys())[::-1]:
        for num in pars[orden]:
            text=re.sub('\\b'+num+'\\b','#', text)
    return text


#________________________________________________________________________________________________________
def cleantext(text, stopwords):
    # mapa de sustitucion para acentos y diéresis
    allowedch={225: 97, 233: 101, 237: 105, 243: 111, 250: 117, 193: 97,
     201: 101, 205: 105, 211: 111, 218: 117, 228: 97,
     235: 101, 239: 105, 246: 111, 252: 117, 196: 97,
     203: 101, 207: 105, 214: 111, 220: 117, 224: 97, 
     232: 101, 236: 105, 242: 111, 249: 117}
    
    # quita tags html
    text=re.sub('</?[^>]+>', ' ', text)
    # quita uniones de palabras por puntos, diagonales o : incluye urls y horas y fechas separadas por : y /
    text=re.sub('(\S+[/\.:](\S+))+\S+\s', ' ', text)
    # quita numeros en letra
    text=numparser(text)
    # quita caracteres que no son letras
    text=re.sub('[\s\?¿!¡=°\|¬\$%\&/\(\)\*\\\+¨´^`\{;,\:\.\}\[\]“”\-_#@]', ' ', text)
    
    # quita saltos de linea y saltos y espacios seguidos
    text=re.sub('[\n\r\t]+', ' ', text)
    # quita comillas por errores en query
    text=text.replace('"','').replace("'","")
    # Bloque para quitar emojis
    # pasamos los caracteres con puntuación a sin puntuación (á->a) depende del diccionaio que se tenga
    text=text.translate(allowedch)
    # en el mismo paso se convierte a utf-8 y se reemplazan las ñ por ñ (truco para que al quitar caracteres raros no reemplace las ñ)
    text=re.sub('(\\\\xc3\\\\xb1)|(\\\\xc3\\\\x91)', 'ñ',str(text.encode('utf-8')))
    # se quitan los caracteres raros pero como la ñ no queda con este formato no se reemplaza :P
    text=re.sub('\\\\x[a-z\d]{2}', '',text)
    # quita los caracteres extra generados en el proceso anterior
    # originalmente son comillas pero varía si la comilla es " o '
    text=re.findall("^b\W(.*)\W$", text)[0]
    
    # separa palabras compuestas tipo camelCase utiles para separar hashtags, palabras tipo 6Sigma
    matches=['([a-z][A-Z])[a-zA-Z]','([a-zA-Z]\d)', '(\d[a-zA-Z])']
    for match in matches:
        joints=re.findall(match, text)
        for joint in joints:
            text=re.sub(joint, joint[0]+' '+joint[1], text)
    
    #quita palabras de longitud 1
    #text=re.sub('\s+\w{1}\s+', ' ', text)
    # quita numeros aislados excepto de 4 cifras que pueden ser años
    text=re.sub('(?:\\b\d{1,3}\\b)|(?:\\b\d{4,}\\b)', ' ', text)
    # quita stopwords
    stopwords='\\b|\\b'.join(stopwords)
    stopwords='\\b'+stopwords+'\\b'
    text=re.sub(stopwords,'', text)
    
    # quita espacios juntos y los sustituye por uno solo
    text=re.sub('\s{2,}', ' ', text)
    # quita ultimo y primer espacio
    text=text.strip()
    return text

def parserquery(query):
    match=''


#______________________________________________________________
def runkwds(kwdsin: list, kwdsnotin: list, textos: list) -> list:
    if kwdsin:
        if not kwdsnotin:
            kwdsnotin=[]
        
        vect=CV()
        vect.fit(kwdsin+kwdsnotin)
    else:
        vect=pretrained_model
    
    #vocab=dict(sorted(zip(vect.vocabulary_.values(),vect.vocabulary_.keys())))
    s=pd.Series(textos)
    s=s.map(lambda x: cleantext(x, stopwords))
    
    r=vect.transform(s).toarray()
    
    try:    
        kin=vect.transform(kwdsin).toarray()[0]
        docsin=set(r.dot(kin).nonzero()[0])
    except:
        print('esto no es posible')
        return []
    try:
        kno=vect.transform(kwdsnotin).toarray()[0]
        docsno=set(r.dot(kno).nonzero()[0])
    except:
        print('no hay palabras no incluidas')
        docsno=set()
    
    s=s[list(docsin-docsno)]
    s.map(lambda x: applyregex(x, kwdsin))
    return list(docsin-docsno)

def applyregex(text: str, rkws: list) -> list:
    return re.findall('|'.join(rkws), text)

#______________________________________________________________
def rekwds(*args, **kwargs):
    future = executor.submit(runkwds, kwargs['kwdsin'], kwargs['kwdsnotin'], kwargs['textos'])    
    res=future.result()
    return res
    
#______________________________________________________________    
@app.post('/findkwds', status_code=201)
async def findkwds(master_id: str = Header(...), target: List[str] = Body(...), kwdsin: Optional[List[str]] = Body(default=None), kwdsnotin: Optional[List[str]]  = Body(default=None), query: str = Body(...)):
    if master_id=='12345':
        res= rekwds(kwdsin=kwdsin, kwdsnotin=kwdsnotin, textos=target, query=query)
        return str(res)

#______________________________________________________________    
if __name__=='__main__':
    
    '''if not dotenv.load_dotenv():
        print('no se encuentran las variables de entorno')
        #raise FileNotFoundError
    
    key_value = input("Ingrese el key pass:")
    
    try:
        if os.environ[key_value]:
            executor=ThreadPoolExecutor(max_workers = 5)
        else:
            raise KeyError
        
    except Exception as e:
        print(e)
        print('No puede ingresar sin el key pass')
        raise KeyError
    '''
    
    pretrained_model=CV()
    pretrained_model.fit(['huracan otis acapulgo guerrero'])
    
    with open('stopwords.txt', 'rb') as f:
        stopwords=f.read().decode('utf-8')
        stopwords=re.split(',\r\n?', stopwords)
        
    executor=ThreadPoolExecutor(max_workers=5)
    
    uvicorn.run(app, host='0.0.0.0', port=8000)
    executor.shutdown()
