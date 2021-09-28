import pandas as pd
import numpy as np
import holidays
from scipy import stats
from datetime import datetime, timedelta
import os
import json
import pyodbc
import sys

class lib_clase:
    """
    Instantiate a Class.
    """
    
    def __init__(self, date_hoy=None):
        self.date_hoy = date_hoy
        
    @staticmethod
    
    def multiplicador(numero, multiplo):
        """
        Multiply a given number by a given multiplier.
    
        :param number: The number to multiply.
        :type number: int
    
        :param muiltiplier: The multiplier.
        :type muiltiplier: int
        """
        
        return numero * multiplo
        
    @staticmethod
        
    def feriado(f):
        """Entrega si una fecha es feriado en Chile

        Args:
            string: una fecha como string

        Returns:
            Boolean (True/False)
        """

        A = f in holidays.Chile()
        return A
    
    @staticmethod    
    def feriados_Chile(year):
        """Entrega los feriados en Chile para un año dado

        Args:
            int: el año para el cual se desean saber los años

        Returns:
            DataFrame con los feriados de un año
        """
        y = [x for x in holidays.Chile(years = year).items() ]
        dates = [a[0] for a in y]
        feriados = [a[1] for a in y]  
        df = pd.DataFrame({'Fecha': dates, 'Feriados': feriados})
        df['Fecha'] = pd.to_datetime( df['Fecha'] )
        return df
    
    @staticmethod    
    def diaantesdeferiado(f):
        """Confirma si el dia dado precede a un feriado

        Args:
            datetime: fecha del dia considerado como objeto datetime de python

        Returns:
            1 si es True, 0 si es False
        """
        if  (f + timedelta(days=1) in holidays.Chile()) & (f not in holidays.Chile()):
            return 1
        else:
            return 0
            
    def percentile(n):
        """Percentil

        Args:

        Returns:
            percentil
        """

        def percentile_(x,n):
            return np.percentile(x, n)
        percentile_.__name__ = 'percentile_%s' % n
        return percentile_
    
    @staticmethod
    def MAD(x):
        """Median Absolute Deviation

        Args:
            Numbers

        Returns:
            Median Absolute Deviation
        """
        B=stats.median_absolute_deviation(x)
        return B
    
    @staticmethod
    def edad_rango(df, col_edad ="EDAD1", col_target = 'RNG' ):
        """Dado un dataframe con una columna de edades, esta funcion las agrupa en segmentos de 5 años.

        Args:
            df: dataframe
            col_edad: nombre de la columna con la edad
            col_target: nombre de la nueva columna con segmentos de edades

        Returns:
            Crea una nueva columna en el dataframe
        """

        idx2025 =  df[col_edad] < 25
        idx2530 = (df[col_edad] >= 25) & (df[col_edad] < 30)
        idx3035 = (df[col_edad] >= 30) & (df[col_edad] < 35)
        idx3540 = (df[col_edad] >= 35) & (df[col_edad] < 40)
        idx4045 = (df[col_edad] >= 40) & (df[col_edad] < 45)
        idx4550 = (df[col_edad] >= 45) & (df[col_edad] < 50)
        idx5055 = (df[col_edad] >= 50) & (df[col_edad] < 55)
        idx5560 = (df[col_edad] >= 55) & (df[col_edad] < 60)
        idx6065 = (df[col_edad] >= 60) & (df[col_edad] < 65)
        idx6570 = (df[col_edad] >= 65) & (df[col_edad] < 70)
        idx7075 = (df[col_edad] >= 70) & (df[col_edad] < 75)
        idx7580 = (df[col_edad] >= 75) & (df[col_edad] < 80)
    #   idx8085 = (df[col_edad] >= 80) & (df[col_edad] < 85)
    #   idx8590 = (df[col_edad] >= 85) & (df[col_edad] < 90)
    #   idx9095 = (df[col_edad] >= 90) & (df[col_edad] < 95)
    #   idx95100 = (df[col_edad] >= 95) & (df[col_edad] <= 100)
        idx100 = df[col_edad] >= 80
        
        df["RNG"] = [""]*len(df)
        df["RNG"].loc[idx2025] = "18_24"
        df["RNG"].loc[idx2530] = "25_29"
        df["RNG"].loc[idx3035] = "30_34"
        df["RNG"].loc[idx3540] = "35_39"
        df["RNG"].loc[idx4045] = "40_44"
        df["RNG"].loc[idx4550] = "45_49"
        df["RNG"].loc[idx5055] = "50_54"
        df["RNG"].loc[idx5560] = "55_59"
        df["RNG"].loc[idx6065] = "60_64"
        df["RNG"].loc[idx6570] = "65_69"
        df["RNG"].loc[idx7075] = "70_74"
        df["RNG"].loc[idx7580] = "75_79"
        df["RNG"].loc[idx100] = "sobre_80"
    #    df["RNG"].loc[idx8590] = "85_89"
    #    df["RNG"].loc[idx9095] = "90_94"
    #    df["RNG"].loc[idx95100] = "95_100"
    #    df["RNG"].loc[idx100] = "sobre_100"
        
        return df
    
    @staticmethod
    def edad_rango_vectorcliente(df, col_edad ="edad1", col_target = 'RNG' ):    
        """Dado un dataframe con una columna de edades, esta funcion las agrupa en segmentos de 5 años.

        Args:
            df: dataframe
            col_edad: nombre de la columna con la edad
            col_target: nombre de la nueva columna con segmentos de edades

        Returns:
            Crea una nueva columna en el dataframe
        """


        idx1825 =  df[col_edad] < 25
        idx2530 = (df[col_edad] >= 25) & (df[col_edad] < 30)
        idx3035 = (df[col_edad] >= 30) & (df[col_edad] < 35)
        idx3540 = (df[col_edad] >= 35) & (df[col_edad] < 40)
        idx4045 = (df[col_edad] >= 40) & (df[col_edad] < 45)
        idx4550 = (df[col_edad] >= 45) & (df[col_edad] < 50)
        idx5055 = (df[col_edad] >= 50) & (df[col_edad] < 55)
        idx5560 = (df[col_edad] >= 55) & (df[col_edad] < 60)
        idx6065 = (df[col_edad] >= 60) & (df[col_edad] < 65)
        idx6570 = (df[col_edad] >= 65) & (df[col_edad] < 70)
        idx7075 = (df[col_edad] >= 70) & (df[col_edad] < 75)
        idx7580 = (df[col_edad] >= 75) & (df[col_edad] < 80)
        idx8085 = (df[col_edad] >= 80) & (df[col_edad] < 85)
        idx8590 = (df[col_edad] >= 85) & (df[col_edad] < 90)
        idx9095 = (df[col_edad] >= 90) & (df[col_edad] < 95)
        idx95100 = (df[col_edad] >= 95) & (df[col_edad] <= 100)
        idx100 = df[col_edad] > 100
        
        df[col_target] = ["sin_edad"]*len(df)
        df.loc[idx1825,col_target] = "18_24"
        df.loc[idx2530,col_target] = "25_29"
        df.loc[idx3035,col_target] = "30_34"
        df.loc[idx3540,col_target] = "35_39"
        df.loc[idx4045,col_target] = "40_44"
        df.loc[idx4550,col_target] = "45_49"
        df.loc[idx5055,col_target] = "50_54"
        df.loc[idx5560,col_target] = "55_59"
        df.loc[idx6065,col_target] = "60_64"
        df.loc[idx6570,col_target] = "65_69"
        df.loc[idx7075,col_target] = "70_74"
        df.loc[idx7580,col_target] = "75_79"
        df.loc[idx8085,col_target] = "80_84"
        df.loc[idx8590,col_target] = "85_89"
        df.loc[idx9095,col_target] = "90_94"
        df.loc[idx95100,col_target] = "95_100"
        df.loc[idx100,col_target] = "sobre_100"
    
        return df
        
        
         #Funcion para corregir fechas de cuarentena
    @staticmethod
    def correct_cuarentena(fecha_ano_anterior, fecha_hoy):
        """"
        Funcion para corregir fechas tomando en cuenta las dos cuarentenas
        fecha_ano_anterior: Fecha inicial del periodo
        fecha_hoy : Fecha final del periodo
        """

        #Convertir a formato de fecha
        if type(fecha_ano_anterior) == str:
            fecha_ano_anterior = pd.to_datetime(fecha_ano_anterior)
        if type(fecha_hoy) == str:
            fecha_hoy = pd.to_datetime(fecha_hoy)

        cuarentena_start = pd.to_datetime('2020-03-18')  # primer dia de cuarentena
        cuarentena_end = pd.to_datetime('2020-11-18')  # ultimo dia de cuarentena
        cuarentena_start2 = pd.to_datetime('2021-04-08')  # primer dia de 2da cuarentena
        cuarentena_end2 = pd.to_datetime('2021-05-23')  # ultimo dia de 2da cuarentena

        # Preparamos la variable
        fecha_ano_anterior_RES = fecha_ano_anterior

        # Para evitar posibles errores
        if (fecha_ano_anterior > fecha_hoy):
            sys.exit('Error: Fecha inicial no puede ser mayor a fecha final')

        # Inicio de correcciones
        #### Primero corregimos por 2NDA CUARENTENA ####

        # 1er caso: No hay necesidad de corregir por la 2da cuarentena
        if (fecha_ano_anterior > cuarentena_end2) | (fecha_hoy < cuarentena_start2):
            print('No hay correccion por 2da cuarentena')

        # 2do caso: fecha_ano_anterior cae dentro de 2nda cuarentena
        elif (fecha_ano_anterior >= cuarentena_start2) & (fecha_ano_anterior <= cuarentena_end2):
            dT = (cuarentena_end2 - fecha_ano_anterior) + pd.Timedelta(days=1)
            fecha_ano_anterior_RES = cuarentena_start2 - dT

        # 3er caso: hay que corregir la 2da cuarentena completa
        elif (fecha_ano_anterior < cuarentena_start2) & (fecha_hoy > cuarentena_end2):
            dT2 = (cuarentena_end2 - cuarentena_start2) + pd.Timedelta(days=1)
            fecha_ano_anterior_RES = fecha_ano_anterior_RES - dT2

        else:
            sys.exit('Error en correccion de 2da cuarentena, revisar correct_cuarentena')

        #### Ahora corregimos por la 1ERA CUARENTENA ####

        # 4to caso: no hay que corregir por la 1era cuarentena
        if (fecha_ano_anterior_RES > cuarentena_end) | (fecha_hoy < cuarentena_start):
            print('No hay correccion por 1era cuarentena')

        # 5to caso: fecha_ano_anterior cae dentro de 1era cuarentena
        elif (fecha_ano_anterior_RES >= cuarentena_start) & (fecha_ano_anterior_RES <= cuarentena_end):
            dT = (cuarentena_end - fecha_ano_anterior_RES) + pd.Timedelta(days=1)
            fecha_ano_anterior_RES = cuarentena_start - dT

        # 6to caso: hay que corregir por la 1era cuarentena completa
        elif (fecha_ano_anterior_RES < cuarentena_start) & (fecha_hoy > cuarentena_end):
            dT1 = (cuarentena_end - cuarentena_start) + pd.Timedelta(days=1)
            fecha_ano_anterior_RES = fecha_ano_anterior_RES - dT1

        else:
            sys.exit('Error en correccion de 1era cuarentena, revisar correct_cuarentena')


        ### Chequeo de que el resultado no caiga en alguna de las dos cuarentenas
        if ((fecha_ano_anterior_RES >= cuarentena_start) & (fecha_ano_anterior_RES <= cuarentena_end)) | \
                ((fecha_ano_anterior_RES >= cuarentena_start2) & (fecha_ano_anterior_RES <= cuarentena_end2)):

            sys.exit('Error en correccion de cuarentenas, revisar correct_cuarentena')


        return fecha_ano_anterior_RES.strftime("%Y-%m-%d"), fecha_hoy.strftime("%Y-%m-%d")
            
#%% Funciones para hacer SQL querys 

        
    def sql_imercado(self, SQL, task = "query", data = None, dictio_credenciales = 'env_dictio.json', verbosear = True ):
        """Funcion para correr un SQL query en el servidor de imercado. Debe haber un diccionario con las credenciales para conectarse.

        Args:
            SQL = el query a ejecutar, en formato de string
            task = la tarea a ejecutar, puede ser "query" para un query, "insert" para insertar data, o "create" para crear una tabla
            data = para la tarea de insertar data, es la data a insertar
            dictio_credenciales = nombre del diccionario con credenciales para conectarse al servidor. Debe estar en el mismo folder donde uno esta trabajando.

        Returns:
            Para el query, entrega un dataframe. Para los otros tasks, no entrega nada.
        """
        if verbosear:
            print('Corriendo sql query')           
        
        with open(os.path.join( os.getcwd(), dictio_credenciales ), "rb") as f:
            dictio = json.load(f)

        server_imercado = dictio["server_imercado"]
        db_imercado= dictio["db_imercado"]
        username_imercado = dictio["username_imercado"]
        password_imercado = dictio["password_imercado"]
        driver_imercado = dictio["driver_imercado"]      
      
        if task == "query":
            conn = pyodbc.connect('DRIVER='+driver_imercado+';SERVER='+server_imercado+';DATABASE='+db_imercado+';UID='+username_imercado+';PWD='+password_imercado)
            cursor = conn.cursor()
    
            sqlstr = str(SQL)
            df = pd.read_sql(sqlstr, con= conn)
    
            cursor.close()
            conn.close()
            if verbosear:
                print("cerrando sql conn")
            return df 

        elif task == "insert":
            conn = pyodbc.connect('DRIVER='+driver_imercado+';SERVER='+server_imercado+';DATABASE='+db_imercado+';UID='+username_imercado+';PWD='+ password_imercado)
            cursor = conn.cursor()

            params = list(tuple(row) for row in data.values)       
            sqlstr = str(SQL)
            cursor.executemany(sqlstr,params)
            conn.commit()

            cursor.close()
            conn.close()
            if verbosear:
                print("cerrando sql conn")
            return
        
        elif task == "create":       
            conn = pyodbc.connect('DRIVER='+driver_imercado+';SERVER='+server_imercado+';DATABASE='+db_imercado+';UID='+username_imercado+';PWD='+ password_imercado)
            cursor = conn.cursor()

            sqlstr = str(SQL)      
            cursor.execute(sqlstr)
            conn.commit()

            cursor.close()
            conn.close()
            if verbosear:
                print("cerrando sql conn")
            return

        else:
            print("Favor indicar un valor adecuado para el parametro task")