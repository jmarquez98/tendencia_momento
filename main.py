import sys
import os
import math
from calendar import monthrange
import dataframe_image as dfi
import numpy as np
import quantstats as qs
from datetime import datetime
import copy
import quantstats as qs
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




# Clase de ETFs ("GEOGRAFIA", "INDUSTRIAS", "FACTORES", "DEUDA", "COMMODITIES")
tipo = "GEOGRAFIA"

# ponderacion puede ser "equal" o "volatility" segun si se quiere asignar igual ponderacion a cada ETF o si pondera por volatilidad
#ponderacion = "equal"

# short puede ser True o False segun si se quiere shortear aquellos activos de tendencia y momentum negativos
short = True
es_short = "short" if short else "efvo"


# Elimino reportes previos y creo carpetas si es necesario
ut.prepare_folders(short=es_short, tipo_etf=tipo)


# Consigo el dataframe correspondiente a los ETFs
data_etfs = ut.get_etfs(tipo_etf=tipo)
tickers = data_etfs.index.to_list()

# Descargo precios historicos de los ETFs
force_alt = True
collection = db.test_collection
data, fallas_t = li.load_function(tickers, collection, li.load_price_info,
                                         li.load_connectionless_info_prices_propio, force_alt=force_alt, cant_t=10, ruedas_fallas=60)
if not force_alt: li.append_last_value(data)

# Si fallo la descarga de algun ETF, levanto error
if len(fallas_t)>0:
    raise ValueError("Fallo la descarga de los siguientes ETFs:", fallas_t)

tickers = [ticker for ticker in tickers if ticker not in fallas_t]


# Descargo la serie historica de tasas a 3 meses
data_t3m = download_fred("TB3MS")


# Mensualizo series de precios y agrego columnas de retornos y volatilidad
data = ut.prepare_data(precios=data)


# Creo dataframe con los retornos de 12 meses (tendencia) para cada ticker y mes
tendencias = ut.get_trends_df(data=data, trend_window=12, save_df=True)

# Obtengo un diccionario cuyas keys son fechas a las cuales les corresponde un diccionario con dos keys: long y short.
# A cada una de las dos keys les corresponde una lista de tickers, los que se van long y short respectivamente
inversiones = ut.get_trend_and_momentum(retornos=tendencias)


# Dataframe cuyas filas son fechas y las columnas son tickers. Para cada fecha, los tickers que no van long ni short
# contienen un cero. Los que van long tienen su retorno del mes, y los que se van short su retorno multiplicado por -1.
posicion = ut.get_returns_df(data=data, inversiones=inversiones)


# Dataframe cuyas filas son fechas y columnas son tickers, en cada celda esta el retorno mensual del ETF.
benchmark = ut.get_trends_df(data=data, trend_window=1, save_df=False).T.iloc[1:]

"""
# Guardo reportes de cada ETF
for ticker in tickers:
    path_rep = path_reporte + "/" + ticker + ".html"
    qs.reports.html(returns=posicion[ticker], benchmark=benchmark[ticker], output=path_rep, title=ticker)
"""


for ponderacion in ["equal", "volatility"]:

    # Obtengo un dataframe de las dimensiones de posicion, que contiene en cada celda la ponderacion que se lleva cada ETF.
    pond_df = ut.get_weights_df(data=data, fechas=posicion.index, pos="long/short", inversiones=inversiones, weight_type=ponderacion)

    # Obtengo un dataframe de las dimensiones de benchmark, que contiene en cada celda la ponderacion que se lleva cada ETF.
    benchmark_pond_df = ut.get_weights_df(data=data, fechas=benchmark.index, pos="all", weight_type=ponderacion)


    # Pondero los retornos historicos de cada ETF
    posicion = posicion * pond_df
    posicion["Return"] = posicion.sum(axis=1)
    posicion["factor"] = 1 + posicion["Return"]
    posicion["acum"] = posicion["factor"].cumprod()

    # Lo mismo para el benchmark
    benchmark = benchmark * benchmark_pond_df
    benchmark["Return"] = benchmark.sum(axis=1)
    benchmark["factor"] = 1 + benchmark["Return"]
    benchmark["acum"] = benchmark["factor"].cumprod()


    # Guardo reporte final del portafolio
    path_rep = path + f"/modelos_{es_short}/{tipo}/{tipo}_{es_short}_{ponderacion}.html"
    titulo = f"MOMENTUM & TREND - {tipo}_{es_short}_{ponderacion} weighting"
    qs.reports.html(returns=posicion["Return"], benchmark=benchmark["Return"], output=path_rep, title=titulo)