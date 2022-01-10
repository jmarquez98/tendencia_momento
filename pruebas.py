import sys
import os
import math
from calendar import monthrange
import dataframe_image as dfi
import numpy as np
import quantstats as qs
from datetime import datetime
import pytz
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



end = datetime.now(pytz.timezone("America/Argentina/Buenos_Aires"))
end_string = datetime.strftime(end, "%Y-%m-%d %H:%M:%S")


print(end_string)


"""
# Clase de ETFs ("GEOGRAFIA", "INDUSTRIAS", "FACTORES", "DEUDA", "COMMODITIES")
tipo = "GEOGRAFIA"

# Consigo el dataframe correspondiente a los ETFs
data_etfs = ut.get_etfs(tipo_etf=tipo)


print(data_etfs)

"""


"""
    # Pondero los retornos que aporta cada ETF
    if ponderacion=="equal":
        # El retorno de la fecha, segun ponderacion equitativa, es igual al promedio de los retornos de cada ETF long/short.
        posicion["Return"] = posicion.replace(0, np.nan).mean(axis=1)
    
    elif ponderacion=="volatility":
        pond_df = pd.DataFrame(columns=tickers)
        for i in range(1, len(fechas)):
            hoy = fechas[i]
            ayer = fechas[i - 1]
    
            longs = inversiones[ayer]["long"]
            shorts = inversiones[ayer]["short"]
    
            pond_df.loc[hoy] = 0  # Parto de un retorno=0 para todos los ETFs, luego modificare cada celda segun corresponda.
    
            # Si para una fecha no hay activos long ni short, entonces el retorno total de esa fecha sera cero.
            if len(longs + shorts) == 0: continue
    
            pond = 0  # Le ire sumando el retorno de cada ETF en la fecha
            den = 0
            for ticker in longs+shorts:
                if round(data[ticker].loc[hoy, "Return"], 8)==0: continue
                vol = data[ticker].loc[hoy, "Volatility"]
                vol_pond = 1 / vol
                pond_df.loc[hoy, ticker] = vol_pond
                den += vol_pond
    
            pond_df.loc[hoy] = pond_df.loc[hoy] / den
    
        posicion = posicion * pond_df
        posicion["Return"] = posicion.sum(axis=1)


"""