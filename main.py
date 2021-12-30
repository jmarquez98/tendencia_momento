import sys
import os
import math
from calendar import monthrange
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
from download_functions import download_fred

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
if not force_alt: li.append_last_value(precios)

if len(fallas_t)>0: raise ValueError("Fallo en descargar algun ETF.")

tickers = [ticker for ticker in tickers if ticker not in fallas_t]

print(fallas_t)



data_t3m = download_fred("TB3MS")

data = {}
for ticker in precios:
    df = copy.deepcopy(precios[ticker])
    df = mg.get_dateValues(dates_df=df, last_dates=5000, period="M")
    df["Return"] = df["Close"].pct_change()
    data[ticker] = df



retornos = pd.DataFrame()
for ticker in data:
    df = data[ticker].copy(deep=True)

    df["Return_12"] = df["Close"].pct_change(12)
    df[ticker] = df["Return_12"]

    df.to_excel(path + f"/etf_dfs/{ticker}.xlsx")

    retornos = pd.concat([retornos, df[[ticker]]], axis=1)

retornos.sort_index(ascending=True, inplace=True)
retornos = retornos.T


fechas = retornos.columns.to_list()
inversiones = {}
for fecha in fechas:

    tabla = retornos.copy(deep=True)
    tabla.dropna(inplace=True, subset=[fecha])
    tabla.sort_values(by=fecha, ascending=False, inplace=True)

    cut = math.ceil(len(tabla) * 0.25)

    longs = tabla.iloc[:cut]
    shorts = tabla.iloc[-cut:]

    longs = longs.loc[longs[fecha] > 0]
    shorts = shorts.loc[shorts[fecha] < 0]

    tickers_l = longs.index.to_list()
    tickers_s = shorts.index.to_list()

    inversiones[fecha] = {"long": tickers_l, "short": tickers_s}


log = pd.DataFrame()
posicion = pd.DataFrame(columns=tickers)
ret_acum = 1
for i in range(1, len(fechas)):
    hoy = fechas[i]
    ayer = fechas[i-1]

    longs = inversiones[ayer]["long"]
    shorts = inversiones[ayer]["short"]

    posicion.loc[hoy] = 0


    if len(longs+shorts)==0:
        log.loc[hoy, "Return"] = 0
        log.loc[hoy, "Acum"] = ret_acum
        continue

    ret = 0
    den = 0
    for ticker in longs:
        retorno = data[ticker].loc[hoy, "Return"]
        posicion.loc[hoy, ticker] = retorno
        if round(retorno, 8)!=0: den += 1 # No quiero que me tome falseamente como positivo/negativo un cero
        ret += retorno

    for ticker in shorts:
        retorno = data[ticker].loc[hoy, "Return"]
        posicion.loc[hoy, ticker] = -retorno
        if round(retorno, 8) != 0: den += 1 # No quiero que me tome falseamente como positivo/negativo un cero
        ret -= retorno



    ret = ret / den
    log.loc[hoy, "Return"] = ret

    # if hoy not in data_t3m.index: continue
    """
    tasa_3m = data_t3m.loc[hoy, "TB3MS"] / 100
    tasa_3m = tasa_3m * monthrange(hoy.year, hoy.month)[1] / 360
    ret = ret - tasa_3m
    """

    ret_acum *= (1+ret)
    log.loc[hoy, "Acum"] = ret_acum


posicion["Return"] = posicion.replace(0, np.nan).mean(axis=1)
posicion["factor"] = 1 + posicion["Return"]
posicion["acum"] = posicion["factor"].cumprod()
posicion.to_excel("posicion.xlsx")

print(ret_acum)

log.to_excel(path + "/logs/log.xlsx")
ret_acum = ret_acum**(12/len(fechas)) - 1

print(round(ret_acum*100, 2), "%")