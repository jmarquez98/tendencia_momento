import sys
import os
import math
import dataframe_image as dfi
import numpy as np
import quantstats as qs
from datetime import datetime
import copy
import pandas as pd
from pandas.core.common import SettingWithCopyWarning
import warnings
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
import pathlib
path = str(pathlib.Path(__file__).parent.absolute())

from funciones import utils as ut

sys.path.insert(1, "../database")
from db_connection import db
import load_info as li
import store_info as sidb

sys.path.insert(1, "../common")
import moduloGenerico as mg

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)





tipo = "GEOGRAFIA"

data_etfs = ut.get_etfs(tipo_etf=tipo)
tickers = data_etfs.index.to_list()


force_alt = True
collection = db.test_collection
precios, fallas_t = li.load_function(tickers, collection, li.load_price_info,
                                         li.load_connectionless_info_prices_propio, force_alt=force_alt, cant_t=10, ruedas_fallas=60)
if not force_alt: li.append_last_value(data)

tickers = [ticker for ticker in tickers if ticker not in fallas_t]

data = {}
for ticker in precios:
    df = copy.deepcopy(precios[ticker])
    df = mg.get_dateValues(dates_df=df, last_dates=5000, period="M")
    data[ticker] = df



retornos = pd.DataFrame()
for ticker in data:
    df = data[ticker].copy(deep=True)

    df["Return"] = df["Adj Close"].pct_change()
    df[ticker] = df["Return"]

    retornos = pd.concat([retornos, df[[ticker]]], axis=1)

retornos.sort_index(ascending=True, inplace=True)
retornos = retornos.T


fechas = retornos.columns.to_list()
inversiones = {}
for fecha in fechas:
    inversiones[fecha] = {}

    tabla = retornos.copy(deep=True)
    tabla.dropna(inplace=True)
    tabla.sort_values(by=fecha, ascending=False)

    cut = math.ceil(len(tabla) * 0.25)

    longs = tabla.iloc[:cut]
    shorts = tabla.iloc[-cut:]


