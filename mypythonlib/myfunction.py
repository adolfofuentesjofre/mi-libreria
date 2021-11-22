import pandas as pd
import numpy as np
import holidays
from scipy import stats
from datetime import datetime, timedelta
import os
import json
import pyodbc
import sys
from scipy.stats import linregress
from multiprocess import Pool, cpu_count

class lib_clase:
    """
    Instantiate a Class.
    """
    
    def __init__(self, date_hoy=None):
        self.date_hoy = date_hoy
        
    @staticmethod
    
    def slope(y):
        """
        Calcula la Pendiente de una serie de tiempo de 4 registros.
    
        Args:
            y: 4 Registros

        Returns:
            Pendiente
        """       
        x=[1,2,3,4]
        slope=linregress(x,y)[0]
        return slope
        
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
            True , False
        """
        if  (f + timedelta(days=1) in holidays.Chile()) & (f not in holidays.Chile()):
            return True
        else:
            return False
            
    def percentile(lista,p):
        """Percentil

        Args:
            Lista de numeros
            Percentil que se desea calcular

        Returns:
            percentil
        """
        a=np.percentile(lista,p)
        return a
    
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
    def edad_rango(df, col_edad ="EDAD", col_target = 'RNG' ):
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
    
    def timebetweenvisits(df_cliente):
        
        """Dado un dataframe con una columna Fecha, esta funcion calculará el tiempo entre visitas.

        Args:
            df_cliente: dataframe

        Returns:
            Crea una nueva columna en el dataframe
        """
        fechas = df_cliente['Fecha'].reset_index(drop=True)    
        lista = [fechas[k+1] - fechas[k] for k in range(0,len(fechas)-1)]
        tbv = [f.days for f in lista]
        tbv.insert(0,0)
        df_cliente['time_diff']=tbv
        return df_cliente
    
    @staticmethod
    
    def timebetweenvisits_multiprocesses(lista_df):
        
        """Dado un dataframe con una columna Fecha, esta funcion calculará el tiempo entre visitas.

        Args:
            df_cliente: dataframe
            num_proce: Numero de Procesadores

        Returns:
            Crea una nueva columna en el dataframe
        """
        num_processes = cpu_count()// 2
        with Pool(num_processes) as pool:
            print("Starting multiprocess")          
            results_list_tbv = pool.map(lib_clase.timebetweenvisits,lista_df) 
        return results_list_tbv
    
    @staticmethod   
        

#%% Funciones para corregir periodos de cuarentena
        
    def correct_cuarentena(fecha_ano_anterior, fecha_hoy):
        """"          
        Funcion para corregir fechas tomando en cuenta las dos cuarentenas
        Args:
            fecha_ano_anterior: Fecha inicial del periodo
            fecha_hoy : Fecha final del periodo
        Returns:
            Periodo Corregido
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
    
    ########## Correccion cuarentena para puntos semestrales
    
    @staticmethod
    def correccion_fechas_semestrales(date_past, date_hoy):
        """Dadas dos fechas, la funcion divide el periodo en 4 segmentos con igual dias de juego tomando en cuenta los periodos de cuarentena.

        Args:
            date_past: fecha inicial del periodo
            date_hoy: fecha final del periodo

        Returns:
            lista con 4 tuplas, cada tupla con las fecha inicial y final
        """

        if type(date_past) == str:
            date_past = pd.to_datetime(date_past)
        if type(date_hoy) == str:
            date_hoy = pd.to_datetime(date_hoy)
    
        cuarentena_start = pd.to_datetime('2020-03-18') #primer dia de cuarentena
        cuarentena_end = pd.to_datetime('2020-11-18')   #ultimo dia de cuarentena
        cuarentena_start2 = pd.to_datetime('2021-04-08') #primer dia de 2da cuarentena
        cuarentena_end2 = pd.to_datetime('2021-05-23')   #ultimo dia de 2da cuarentena   
    
        
        if (date_past >= cuarentena_start) & (date_past <= cuarentena_end):
            sys.exit('ERROR EN FECHAS SEMESTRALES, NO SE HA USADO FUNCION CORRECT_CUARENTENA')
            
                    
        if (date_past >= cuarentena_start2) & (date_past <= cuarentena_end2):
            sys.exit('ERROR EN FECHAS SEMESTRALES, NO SE HA USADO FUNCION CORRECT_CUARENTENA')
    
        
        #no necesidad de corregir
        if (date_past > cuarentena_end2) | (date_hoy < cuarentena_start):
            
            lista_fechas = []
            dDays = (date_hoy - date_past)//4
            
            fecha = date_past
            for _ in range(4):
                fecha_plus = fecha + dDays
                lista_fechas.append( (fecha.strftime('%Y-%m-%d'), fecha_plus.strftime('%Y-%m-%d')) )
                
                fecha = fecha_plus
            
            return lista_fechas
        
        else:
            
            if (date_past < cuarentena_start) & (date_hoy > cuarentena_end) & (date_hoy < cuarentena_start2): #solo 1era cuarentena
                dCuarentena = pd.Timedelta(days = 246)
                
            elif (date_past < cuarentena_start) & (date_hoy > cuarentena_end2) : #Ambas cuarentenas
                dCuarentena = pd.Timedelta(days = 246+46)
                
            elif (date_past < cuarentena_start2) & (date_hoy > cuarentena_end2) : #solo 2da cuarentena
                dCuarentena = pd.Timedelta(days = 46)
                
            else:
                sys.exit('Error en calcular los semestres, chequear funcion correccion_fechas_semestrales')
    
            dDays = ((date_hoy - date_past) - dCuarentena)//4
    
    
            #Prepare loop
            lista_fechas = []
    
            fecha = date_past
            for kk in range(4):
                print(kk)
                fecha_plus = fecha + dDays
                
                if (fecha_plus >= cuarentena_start) & (fecha_plus <= cuarentena_end):
                    print('A')
                    dt = fecha_plus - cuarentena_start
                    fecha_plus = cuarentena_end + dt + pd.Timedelta(days=1)                
                    
                    if (fecha_plus >= cuarentena_start2) & (fecha_plus <= cuarentena_end2):     #In case you fall in the 2nd quarantine
                        print('A2')
                        dt = fecha_plus - cuarentena_start2
                        fecha_plus = cuarentena_end2 + dt + pd.Timedelta(days=1)
                    
                elif (fecha_plus >= cuarentena_start2) & (fecha_plus <= cuarentena_end2):
                    print('B')
                    dt = fecha_plus - cuarentena_start2
                    fecha_plus = cuarentena_end2 + dt + pd.Timedelta(days=1)
                    
                elif (fecha_plus >= cuarentena_end2) & (fecha < cuarentena_start2):
                    print('C')
                    dt = cuarentena_end2 - cuarentena_start2
                    fecha_plus = cuarentena_end2 + dt + pd.Timedelta(days=1)
                    
                    
                lista_fechas.append( (fecha.strftime('%Y-%m-%d'), fecha_plus.strftime('%Y-%m-%d')) )
                
                fecha = fecha_plus
                
            return lista_fechas
    

    ##############Funcion para TBV con correccion cuarentena      
    
    @staticmethod
    
    def timebetweenvisits_cuarentena(df_cliente):
        """Calcula los dias entre visitas de un cliente, tomando en cuenta solo los dias de juego y no los periodos de cuarentena.

        Args:
            df_cliente: dataframe de un cliente con una columna "Fecha" que tiene las fechas de cada visita de dicho cliente

        Returns:
            lista con los dias entre visitas
        """
        import pandas as pd
        
        fechas = df_cliente['Fecha'].reset_index(drop=True)
        fechas = pd.to_datetime(fechas)
        
        cuarentena_inicio = pd.to_datetime('2020-03-18')
        cuarentena_fin = pd.to_datetime('2020-11-18')
        delta_cuarentena = pd.Timedelta(days = 246)
        
        cuarentena_inicio2 = pd.to_datetime('2021-04-08') #primer dia de 2da cuarentena
        cuarentena_fin2 = pd.to_datetime('2021-05-23')   #ultimo dia de 2da cuarentena
        delta_cuarentena2 = pd.Timedelta(days = 46)
    
        
        #lista = [fechas[k+1] - fechas[k] for k in range(0,len(fechas)-1)]
        lista = []
        for k in range(0,len(fechas)-1):
            
            #Correccion por 1era cuarentena
            if fechas[k+1] < cuarentena_inicio2:
                if (fechas[k+1] < cuarentena_inicio) | (fechas[k] > cuarentena_fin):    #no hay correccion
                    lista.append( fechas[k+1] - fechas[k] )
                elif (fechas[k] <= cuarentena_inicio) & (fechas[k+1] >= cuarentena_fin): #restar por cuarentena
                    lista.append( fechas[k+1] - fechas[k] - delta_cuarentena)
                else:
                    lista.append( -9999 )   #verificar que no hay errores
            
            #Correccion por 2da cuarentena
            elif (fechas[k+1] > cuarentena_fin2):     
                if (fechas[k] > cuarentena_fin2):       #No hay correccion
                    lista.append( fechas[k+1] - fechas[k] )               
                elif (fechas[k] < cuarentena_inicio2) & (fechas[k] > cuarentena_fin): #restar por cuarentena2
                    lista.append( fechas[k+1] - fechas[k] - delta_cuarentena2)
                elif (fechas[k] < cuarentena_inicio):
                    lista.append( fechas[k+1] - fechas[k] - (delta_cuarentena+delta_cuarentena2))       #restar por ambas cuarentenas
                else:
                    lista.append( -9999 )   #verificar que no hay errores                
                        
                
    
        tbv = [f.days for f in lista]        
        return tbv


            
#%% Conectores

        
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

    def sql_azure(self, SQL, task = "query", data = None, dictio_credenciales = 'env_dictio.json', verbosear = True ):
        """Funcion para correr un SQL query en el servidor de Azure. Debe haber un diccionario con las credenciales para conectarse.

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

        server_az = dictio["server_az"]
        db_az= dictio["db_az"]
        username_az = dictio["username_az"]
        password_az = dictio["password_az"]
        driver_az = dictio["driver_az"]
        
        if task == "query":
            conn = pyodbc.connect('DRIVER='+driver_az+';SERVER='+server_az+';DATABASE='+db_az+';UID='+username_az+';PWD='+ password_az)
            cursor = conn.cursor()
    
            sqlstr = str(SQL)
            df = pd.read_sql(sqlstr, con= conn)
    
            cursor.close()
            conn.close()
            if verbosear:
                print("cerrando sql conn")
            return df 


        else:
            print("Favor indicar un valor adecuado para el parametro task")



    def sql_azuresandbox(self, SQL, task = "query", data = None, dictio_credenciales = 'env_dictio.json', verbosear = True ):
        """Funcion para correr un SQL query en el servidor de Azure Sandbox. Debe haber un diccionario con las credenciales para conectarse.

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

        server_az_sb = dictio["server_az_sb"]
        db_az_sb= dictio["db_az_sb"]
        username_az_sb = dictio["username_az_sb"]
        password_az_sb = dictio["password_az_sb"]
        driver_az_sb = dictio["driver_az_sb"]
        
        if task == "query":
            conn = pyodbc.connect('DRIVER='+driver_az_sb+';SERVER='+server_az_sb+';DATABASE='+db_az_sb+';UID='+username_az_sb+';PWD='+ password_az_sb)
            cursor = conn.cursor()
    
            sqlstr = str(SQL)
            df = pd.read_sql(sqlstr, con= conn)
    
            cursor.close()
            conn.close()
            if verbosear:
                print("cerrando sql conn")
            return df 


        else:
            print("Favor indicar un valor adecuado para el parametro task")



    def sql_crm(self, SQL, task = "query", data = None, dictio_credenciales = 'env_dictio.json', verbosear = True ):
        """Funcion para correr un SQL query en el servidor de CRM. Debe haber un diccionario con las credenciales para conectarse.

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

        server_crm = dictio["server_crm"]
        db_crm= dictio["db_crm"]
        username_crm = dictio["username_crm"]
        password_crm = dictio["password_crm"]
        driver_crm = dictio["driver_crm"]
        
        if task == "query":
            conn = pyodbc.connect('DRIVER='+driver_crm+';SERVER='+server_crm+';DATABASE='+db_crm+';UID='+username_crm+';PWD='+ password_crm)
            cursor = conn.cursor()
    
            sqlstr = str(SQL)
            df = pd.read_sql(sqlstr, con= conn)
    
            cursor.close()
            conn.close()
            if verbosear:
                print("cerrando sql conn")
            return df 


        else:
            print("Favor indicar un valor adecuado para el parametro task")

            

#%% Funciones para hacer SQL querys 

    def sqlquery_horariojuego_FDS(self, fecha_inicial, fecha_final, dictio_credenciales = 'env_dictio.json'):
        """Obtener el horario de juego y semana/Fin De Semana de clientes, junto con otros atributos, para cierto periodo de tiempo dado.

        Args:
            fecha_inicial = fecha inicial del periodo a considerar
            fecha_final = fecha final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')
        
        
        fecha_inicial = str(fecha_inicial)
        fecha_final = str(fecha_final)
        
        
        sqlquery = """
            Select A.*, VC.* , DIA.FDS , DIA.SEMANA , DIA.MAÑANA AS MANANA , DIA.TARDE , DIA.NOCHE, DIA.MADRUGADA
            From
            (
            Select PLAYER_ID,
            SUM(Slot_NWin) as Slot, 
            SUM(Mesa_NWin) as Mesa,
            COUNT(DISTINCT( Casino) ) NCASINO,
            COUNT( Gamingdate ) as N_VISITAS
            From [imercado]..[tafd_resumen] tr
            Where tr.Gamingdate between '%s' and '%s'  and (Slot > 0 or Mesa > 0 or Bingo > 0)
            and casino <> 'Mendoza'
            Group By
            player_id) A
            inner join (
            SELECT  ID_MCC,
            EDAD,
            SEGMENTO_FUMADOR
            FROM [imercado].[dbo].[DM_BSE_CLIENTES]
            WHERE ESTADO_CLIENTE in ('Normal','Miembro de honor')
            AND CARTERA_VIGENTE = 'S') VC
            ON A.player_id=VC.ID_MCC
            
            left join (
            
            Select
            PLAYER_ID
            , (SUM (FDS)  + 0.0) / (SUM (FDS) + SUM (Semana)) FDS
            , (SUM (Semana)  + 0.0) / (SUM (FDS) + SUM (Semana)) SEMANA
            , (SUM (MAÑANA)  + 0.0) / (SUM (MAÑANA)+SUM (TARDE)+SUM (NOCHE)+SUM (MADRUGADA)) MAÑANA
            , (SUM (TARDE)  + 0.0) / (SUM (MAÑANA)+SUM (TARDE)+SUM (NOCHE)+SUM (MADRUGADA)) TARDE
            , (SUM (NOCHE)  + 0.0) / (SUM (MAÑANA)+SUM (TARDE)+SUM (NOCHE)+SUM (MADRUGADA)) NOCHE
            , (SUM (MADRUGADA)+ 0.0) / (SUM (MAÑANA)+SUM (TARDE)+SUM (NOCHE)+SUM (MADRUGADA)) MADRUGADA
            
            From
                                                                                                    
            (select
            player_id AS PLAYER_ID
            , case when DATEPART(DW, Gamingdate) IN (6,7,1) then COUNT (distinct gamingdate) else 0 End FDS
            , case when DATEPART(DW, Gamingdate) IN (2,3,4,5) then COUNT (distinct gamingdate) else 0 End Semana
            , Case When ((Hora_Salida + Hora_entrada) / 2) < 12 THEN COUNT (distinct gamingdate) Else 0 End MAÑANA
            , Case When ((Hora_Salida + Hora_entrada) / 2) < 20 and ((Hora_Salida + Hora_entrada) / 2) >= 12 THEN COUNT (distinct gamingdate) Else 0 End TARDE
            , Case When ((Hora_Salida + Hora_entrada) / 2) < 24 and ((Hora_Salida + Hora_entrada) / 2) >= 20 THEN COUNT (distinct gamingdate) Else 0 End NOCHE
            , Case When ((Hora_Salida + Hora_entrada) / 2) < 31 and ((Hora_Salida + Hora_entrada) / 2) >= 24 THEN COUNT (distinct gamingdate) Else 0 End MADRUGADA
            From [imercado]..[tafd_resumen]
            where Gamingdate between '%s' and '%s' and Slot+Mesa+Bingo> 0
            and casino <> 'Mendoza'
            
            group by player_id, DATEPART(DW, Gamingdate), Hora_entrada, Hora_salida
            having (Sum(Slot)+Sum(Mesa)+Sum(Bingo))>0
            )JuegoDia
            
            Group by Player_id ) DIA
            ON A.player_id=DIA.PLAYER_ID""" % (fecha_inicial, fecha_final, fecha_inicial, fecha_final)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df



    #############################
    def sqlquery_categoriatarjeta(self, fecha_inicial, fecha_final, dictio_credenciales = 'env_dictio.json'):
        """Funcion para correr un SQL query y obtener la categoria tarjeta de clientes segun los puntos que tienen dentro de un periodo de tiempo.

        Args:
            fecha_inicial = fecha inicial del periodo a considerar
            fecha_final = fecha final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')

        fecha_inicial = str(fecha_inicial)
        fecha_final = str(fecha_final)
        
        
        sqlquery = """
            Select c.ID_MCC,SUM(Puntos) as PUNTOS,
            CASE
            WHEN SUM(Puntos) < 15000 THEN '5 SILVER'
            WHEN SUM(Puntos) < 80000 AND SUM(Puntos) >= 15000  THEN '4 GOLD'
            WHEN SUM(Puntos) < 250000 AND SUM(Puntos) >= 80000 THEN '3 PLATINUM'
            WHEN SUM(Puntos) < 1500000 AND SUM(Puntos) >= 250000 THEN '2 DIAMOND'
            WHEN SUM(Puntos) >= 1500000  THEN '1 SEVEN STARS'
            END AS CATEGORIA
            From imercado..DM_BSE_CLIENTES c
            join imercado..tafd_resumen tr on c.ID_MCC = tr.player_id
            Where
            tr.Gamingdate between '%s' and '%s'
            and c.CARTERA_VIGENTE = 'S'
            and (Slot > 0 or Mesa > 0 or Bingo > 0)
            and  CASINO != 'Mendoza'
            Group By
            c.ID_MCC
        """ % (fecha_inicial, fecha_final)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df



    #############################
    def sqlquery_puntosyfechas(self, fecha_inicial, fecha_final, dictio_credenciales = 'env_dictio.json'):
        """Funcion para correr un SQL query y obtener los puntos por fecha de juego de cada cliente.

        Args:
            fecha_inicial = fecha inicial del periodo a considerar
            fecha_final = fecha final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')

        
        fecha_inicial = str(fecha_inicial)
        fecha_final = str(fecha_final)
        
        
        sqlquery = """
            select player_id, Slot + Mesa as Puntos, CONVERT(varchar,Gamingdate,112) as Fecha
            From imercado..tafd_resumen tr
            Where tr.Gamingdate between '%s' and '%s'
            and (Slot > 0 or Mesa > 0 or Bingo > 0)
            and  CASINO != 'Mendoza'
            and  player_id in (
            SELECT  ID_MCC
            FROM [imercado].[dbo].[DM_BSE_CLIENTES]
            where ESTADO_CLIENTE in ('Normal','Miembro de honor')
            AND CARTERA_VIGENTE = 'S')
            ORDER BY player_id, Gamingdate
        """ % (fecha_inicial, fecha_final)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df



    #############################
    def sqlquery_fechaactivacion(self, fecha_hoy = None, dictio_credenciales = 'env_dictio.json'):
        """Funcion para correr un SQL query y obtener la fecha de activacion (1era fecha de juego) y la antiguedad.

        Args:
            fecha_hoy = fecha de hoy para calcular la antiguedad, o la fecha del final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')

        
        if fecha_hoy == None:
            fecha_hoy = datetime.now().strftime('%Y-%m-%d')
            print("Calculando antiguedad usando la fecha de hoy de: " + fecha_hoy)
        
        
        sqlquery = """
            select player_id, min(CONVERT(varchar,Gamingdate,112)) as FECHA_ACTIVACION,
            DATEDIFF(day, min(CONVERT(varchar,Gamingdate,112)), '%s'  ) as ANTIGUEDAD_DIAS
            From imercado..tafd_resumen tr
            Where (Slot > 0 or Mesa > 0 or Bingo > 0)
            and  CASINO != 'Mendoza'
            and  player_id in (
            SELECT  distinct ID_MCC
            FROM [imercado].[dbo].[DM_BSE_CLIENTES]
            join imercado..tafd_resumen tr on ID_MCC = player_id
                                Where
                                tr.Gamingdate <= '%s'
                                and (Slot > 0 or Mesa > 0 or Bingo > 0)
                                and  CASINO != 'Mendoza'
            and ESTADO_CLIENTE in ('Normal','Miembro de honor')
            and CARTERA_VIGENTE = 'S')
            group by  player_id
            order by min(CONVERT(varchar,Gamingdate,112))
        """ % (fecha_hoy, fecha_hoy)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df




    #############################
    def sqlquery_Recencia(self, fecha_ano_anterior, fecha_hoy, dictio_credenciales = 'env_dictio.json'):
        """Funcion para correr un SQL query y obtener recencia con info de Scan.

        Args:
            fecha_ano_anterior = fecha inicial del periodo a considerar
            fecha_hoy = fecha final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')

        
        fecha_ano_anterior = str(fecha_ano_anterior)
        fecha_hoy = str(fecha_hoy)
        
        
        sqlquery = """
            Select ID_MCC, max(jornada) AS RECENCIA_SCAN
            from( 
            Select ul.Desc_Unidad am_Unidad
              , am_Rut
              , am_Rut_Limpio
              , c.ID_MCC 
              , c.CATEGORIA_TARJETA
              , c.Segmento_Gestion
              , c.Ejecutivo_Host
              , mes
              ,(CASE WHEN (CONVERT(time, am_FechaMarca) >= '07:00:00') THEN CONVERT(date, am_FechaMarca) ELSE CONVERT(date, DATEADD(day, -1, am_FechaMarca)) END) as jornada 
              , am_FechaMarca Fecha
              
            from 
             (
              SELECT
               am.am_Rut
               , R.cero_pos
               , STUFF(am.am_Rut, 1, R.cero_pos - 1, '') AS am_Rut_Limpio
               , am_Unidad
               , am_FechaMarca
               , month(am_FechaMarca) mes
              FROM
               IMC_MCC..AUTOEXCLUIDOS_MARCAS am
               CROSS APPLY
               (SELECT PATINDEX('%[^0]%', am.am_Rut)) AS R(cero_pos)
              Group By
               am.am_Rut
               , R.cero_pos
               , STUFF(am.am_Rut, 1, R.cero_pos - 1, '') 
               , am_Unidad
               , am_FechaMarca
               , month(am_FechaMarca)
             )am
             Join imercado..dm_grl_unidad_libre ul on am_Unidad = ul.Desc_Libre_Unidad
             Left join
             (
              Select *
              From
              (
               Select distinct RUT, ID_MCC, CATEGORIA_TARJETA,Segmento_Gestion,Ejecutivo_host
               From imercado..DM_BSE_CLIENTES 
               Where RUT IS NOT NULL
              )  a
             )c on am.am_Rut_Limpio = case when LEN(c.RUT) > 1 Then left(c.RUT, LEN(RUT) - 1) ELSE c.RUT End
            Where 
              ---ul.Desc_Unidad = 'Viña' and
              am_Rut_Limpio not in ('0', '1')
              and am_Rut_Limpio is not null
              and ID_MCC is not null
              --and mes=8
              --and month(am_FechaMarca)=8
              --and year(am_FechaMarca)=2019
            ) tot
            where Fecha > '2019-01-01'
            and Fecha < '{}'  
            and ID_MCC in ( select distinct player_id
                From imercado..tafd_resumen tr
                Where tr.Gamingdate between '{}' and '{}'
                and (Slot > 0 or Mesa > 0 or Bingo > 0)
                and  CASINO != 'Mendoza'
                and  player_id in (
                      SELECT  ID_MCC
                      FROM [imercado].[dbo].[DM_BSE_CLIENTES] 
                      where ESTADO_CLIENTE in ('Normal','Miembro de honor')
                                                    and CARTERA_VIGENTE = 'S')
        )group by ID_MCC
        """.format(fecha_hoy,fecha_ano_anterior,fecha_hoy)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df



    #############################
    def sqlquery_canje(self, fecha_final, dictio_credenciales = 'env_dictio.json'):
        """Funcion para correr un SQL query y obtener canje en Kiosko.

        Args:
            fecha_final = fecha final del periodo a considerar, desde 2017-01-01 hasta fecha_final

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')

        
        fecha_final = str(fecha_final)
        
        
        sqlquery = """
            SELECT CANJE.ID_MCC, CANJE.LINEA, SUM(CANJE.MONTO) AS MONTO
            FROM(
            Select ID_MCC
            , CATEGORIA_PRODUCTO
            , TIPO_PRODUCTO
            , MONTO
            , Case
            When TIPO_PRODUCTO in ('JUEGO KIOSCO', 'CHEQUE PESO MESA', 'JUEGO REDENCION', 'CHEQUE PESO TGM', 'TGM REDENCION') Then 'JUEGO'
            Else 'OTRO'
            End LINEA
            , CASINO_IMPRESION
            from imercado..DM_bse_ECC
            Where ORIGEN_BENEFICIO = 'Canje'
            and JORNADA_CANJE between '2017-01-01' and '%s'
            and ID_MCC in
                (SELECT ID_MCC FROM [imercado].[dbo].[DM_BSE_CLIENTES] 
                where CARTERA_VIGENTE = 'S')
            ) CANJE
            GROUP BY CANJE.ID_MCC , LINEA
        """ % (fecha_final)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df


    #############################
    def sqlquery_reinversion(self, fecha_inicial, fecha_final, dictio_credenciales = 'env_dictio.json'):
        """Funcion para correr un SQL query y obtener la reinversion usando tabla de factores dentro del periodo considerado, para los casinos de Antofagasta, Coquimbo, Santiago, y Viña.

        Args:
            fecha_inicial = fecha inicial del periodo a considerar
            fecha_final = fecha final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')

        
        fecha_inicial = str(fecha_inicial)
        fecha_final = str(fecha_final)
        
        
        sqlquery = """
            SELECT *,
            (CASE WHEN A.[Casino Canje] = 'Antofagasta' AND A.Tipo = 'AABB' THEN 0.5858*A.Monto
                WHEN A.[Casino Canje] = 'Antofagasta' AND A.Tipo = 'HOTEL' THEN 0.5146*A.Monto
                WHEN A.[Casino Canje] = 'Antofagasta' AND A.Tipo = 'NCC' THEN 0.6232*A.Monto
                WHEN A.[Casino Canje] = 'Antofagasta' AND A.Tipo = 'FNN' THEN 0.6515*A.Monto
                WHEN A.[Casino Canje] = 'Coquimbo' AND A.Tipo = 'AABB' THEN 0.4978*A.Monto
                WHEN A.[Casino Canje] = 'Coquimbo' AND A.Tipo = 'HOTEL' THEN 0.4352*A.Monto
                WHEN A.[Casino Canje] = 'Coquimbo' AND A.Tipo = 'NCC' THEN 0.6199*A.Monto
                WHEN A.[Casino Canje] = 'Coquimbo' AND A.Tipo = 'FNN' THEN 0.6568*A.Monto
                WHEN A.[Casino Canje] = 'Santiago' AND A.Tipo = 'AABB' THEN 0.8549*A.Monto
                WHEN A.[Casino Canje] = 'Santiago' AND A.Tipo = 'HOTEL' THEN 0.4394*A.Monto
                WHEN A.[Casino Canje] = 'Santiago' AND A.Tipo = 'NCC' THEN 0.6276*A.Monto
                WHEN A.[Casino Canje] = 'Santiago' AND A.Tipo = 'FNN' THEN 0.6526*A.Monto
                WHEN A.[Casino Canje] = 'Viña' AND A.Tipo = 'AABB' THEN 0.5693*A.Monto
                WHEN A.[Casino Canje] = 'Viña' AND A.Tipo = 'HOTEL' THEN 0.3073*A.Monto
                WHEN A.[Casino Canje] = 'Viña' AND A.Tipo = 'NCC' THEN 0.3841*A.Monto
                WHEN A.[Casino Canje] = 'Viña' AND A.Tipo = 'FNN' THEN 0.6446*A.Monto
                ELSE A.Monto END) AS Monto_Real
            FROM(
                SELECT 
                [Id Cliente] Cuenta ,
                Jornada,
                sum(monto) Monto,
                Tipo,
                [Casino Canje]
                From [imercado].[dbo].[DM_Costos_Clientes] 
                Where  (Jornada between '%s' and '%s')  and Tipo in ('NCC', 'FICHA MESA','HOTEL', 'AABB') 
                and [Origen Beneficio]='GESTIÓN'
                and [Id Cliente] <> 0
                and [Casino Canje] in ('Antofagasta','Coquimbo','Santiago','Viña')
                and [Id Cliente] in (
                    SELECT  ID_MCC
                    FROM [imercado].[dbo].[DM_BSE_CLIENTES] 
                    where ESTADO_CLIENTE in ('Normal','Miembro de honor')
                    and CARTERA_VIGENTE = 'S'
                    )
            group by [Id Cliente] , Jornada, Tipo,[linea negocio], [Casino Canje] ) A
            order by  Cuenta , Jornada             
        """ % (fecha_inicial, fecha_final)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df





    #############################
    def sqlquery_numcasinosporcliente(self, fecha_inicial, fecha_final, dictio_credenciales = 'env_dictio.json'):
        """Funcion para correr un SQL query y obtener el numero de casinos distintos por cliente dentro de cierto periodo.

        Args:
            fecha_inicial = fecha inicial del periodo a considerar
            fecha_final = fecha final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')

        
        fecha_inicial = str(fecha_inicial)
        fecha_final = str(fecha_final)
        
        
        sqlquery = """
            Select
            [player_id],
            Count(Distinct(Casino)) Casinos
            FROM [imercado].[dbo].[tafd_resumen]
            WHERE
            (Gamingdate between '%s' and '%s')
            and (Slot > 0 or Mesa > 0 or Slot_Nwin > 0 or Mesa_nWin >0)
            and (Casino <> 'Mendoza')
            and [player_id] in (
             select ID_MCC FROM [imercado].[dbo].[DM_BSE_CLIENTES] 
                where CARTERA_VIGENTE = 'S'
            )
            GROUP BY [player_id]
            order by Count(Distinct(Casino))
        """ % (fecha_inicial, fecha_final)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df



    #############################
    def sqlquery_fichapromoyFNN(self, fecha_inicial, fecha_final, dictio_credenciales = 'env_dictio.json'):
        """Funcion para correr un SQL query y obtener Ficha Promo y FNN para jornadas dentro de un periodo.

        Args:
            fecha_inicial = fecha inicial del periodo a considerar
            fecha_final = fecha final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')

        
        fecha_inicial = str(fecha_inicial)
        fecha_final = str(fecha_final)
        
        
        sqlquery = """
            select
            case
                   when Descripcion_Cupon = 'Ficha Promo' then 'Ficha Promo'
                   when Descripcion_Cupon is null then 'Ficha Promo'
                   when Descripcion_Cupon = 'Match Play' then 'Match Play'
                   else 'FNN'
            end as 'Descripcion_Cupon2',
            Descripcion_Cupon,
            sum(monto) Monto
            From [imercado].[dbo].[DM_Costos_Clientes]
            where
                   ([Casino Canje] not in ('Mendoza'))
            and (Tipo='FICHA MESA')
            and (Jornada between '%s' and '%s')
            group by Descripcion_Cupon
            order by Descripcion_Cupon2
        """ % (fecha_inicial, fecha_final)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df



    #############################
    def sqlquery_descuentoNCCreinversion(self, fecha_inicial, fecha_final, dictio_credenciales = 'env_dictio.json'):
        """Funcion para correr un SQL query y obtener el descuento NCC de reinversion.

        Args:
            fecha_inicial = fecha inicial del periodo a considerar
            fecha_final = fecha final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')


        fecha_inicial = str(fecha_inicial)
        fecha_final = str(fecha_final)
        fecha_inicial, fecha_final = '2019-01-01', '2021-12-31'
        
        sqlquery ="""Declare @Desde as Date    
            Set @Desde =  '{}'
            Declare @Hasta as Date    
            Set @Hasta =  '{}'

            SELECT 
            Cuenta,
            Casino,
            Casino_Cliente,
            Categoria_Tarjeta,
            Estado_Cliente,
            Segmento_Gestion,
            Segmento_Juego,
            Año,
            Mes,
            Sum(Visitas) Visitas,
            Sum(Pts_Tgm) Pts_Tgm,
            Sum(Pts_Mesas) Pts_Mesas,
            Sum(Teorico_Tgm) Teorico_Tgm,
            Sum(Teorico_Mesas) Teorico_Mesas,
            Sum(NCC) NCC,
            Sum(FichaMesa) FichaMesa,
            Sum(Teorico_Tgm)-Sum(NCC) TeoricoTgm_Neto,
            Sum(Teorico_Tgm)+Sum(Teorico_Mesas)-Sum(NCC) Teorico_Neto,
            Sum(Drop1) as 'Drop',
            Sum(WinReal) WinReal
            FROM(
            SELECT 
            [player_id] Cuenta,
            Casino,
            Year(Gamingdate) Año, 
            Month (Gamingdate) Mes, 
            Count(distinct(Gamingdate)) Visitas,
            sum (Slot) Pts_Tgm,
            sum (Mesa) Pts_Mesas,
            sum (Slot_NWin) Teorico_Tgm,
            sum (Mesa_NWin) Teorico_Mesas,
            0 as 'NCC',
            0 as 'FichaMesa', 
            0 as 'Drop1',
            0 as 'WinReal'
            FROM [imercado].[dbo].[tafd_resumen]
            WHERE (Slot>0 or Mesa>0 or Slot_NWin > 0 or Mesa_NWin > 0) and (Gamingdate between @Desde and @Hasta)
            GROUP BY [player_id],Casino,Year(Gamingdate), Month (Gamingdate)
            
            UNION ALL
            
            SELECT 
            [Id Cliente] Cuenta ,
            [Casino Canje] Casino,
            Year(Jornada) Año,
            Month(Jornada) Mes,
            0 as 'Visitas',
            0 as 'Pts_Tgm',
            0 as 'Pts_Mesas',
            0 as 'Teorico_Tgm',
            0 as 'Teorico_Mesas',
            sum (monto) NCC,
            0 as 'FichaMesa', 
            0 as 'Drop1',
            0 as 'WinReal'
            From [imercado].[dbo].[DM_Costos_Clientes] 
            Where  (Jornada between @Desde and @Hasta) and (Tipo='NCC')
            GROUP BY [Id Cliente],[Casino Canje],Year(Jornada),Month(Jornada)
            
            UNION ALL
            
            SELECT 
            [Id Cliente] Cuenta ,
            [Casino Canje] Casino,
            Year(Jornada) Año,
            Month(Jornada) Mes,
            0 as 'Visitas',
            0 as 'Pts_Tgm',
            0 as 'Pts_Mesas',
            0 as 'Teorico_Tgm',
            0 as 'Teorico_Mesas',
            0 as 'NCC',
            sum (monto) FichaMesa, 
            0 as 'Drop1',
            0 as 'WinReal'
            From [imercado].[dbo].[DM_Costos_Clientes] 
            Where  (Jornada between @Desde and @Hasta) and (Tipo='FICHA MESA')
            GROUP BY [Id Cliente],[Casino Canje],Year(Jornada),Month(Jornada)
            
            UNION ALL
            
            SELECT 
            [ID_MCC] Cuenta ,
            CASE 
            WHEN [CASINO_ID] = 1 THEN 'Viña'
            WHEN [CASINO_ID] = 3 THEN 'Coquimbo'
            WHEN [CASINO_ID] = 4 THEN 'Pucón'
            WHEN [CASINO_ID] = 6 THEN 'Antofagasta'
            WHEN [CASINO_ID] = 7 THEN 'Santiago'
            WHEN [CASINO_ID] = 8 THEN 'Chiloé'
            ELSE 'Revisar'
            END AS 'Casino',
            Year(Jornada) Año,
            Month(Jornada) Mes,
            0 as 'Visitas',
            0 as 'Pts_Tgm',
            0 as 'Pts_Mesas',
            0 as 'Teorico_Tgm',
            0 as 'Teorico_Mesas',
            0 as 'NCC',
            0 as 'FichaMesa', 
            Sum([PLACAS_IN]) 'Drop1',
            Sum(WIN) as 'WinReal'
            FROM [IMC_RPT].[dbo].[RPT_Reg_Placa_Datos] 
            WHERE (Jornada between @Desde and @Hasta)
            GROUP BY [ID_MCC],[CASINO_ID],Year(Jornada),Month(Jornada)
            
            ) T1 INNER JOIN [imercado].[dbo].[DM_BSE_CLIENTES] d ON T1.Cuenta = d.ID_MCC
            
            WHERE 
            (Casino in ('Santiago'))
            and (Casino_Cliente = 'Santiago')
            and (Segmento_Gestion like ('%Oriental%'))
            ----and (Categoria_Tarjeta in ('Classic','Silver'))
            ----and (Estado_Cliente = 'Normal')
            --and (Segmento_Gestion like ('%Oriental%'))
            ----and (Segmento_Juego in ('Mesa','-'))
            ----and (segmento_Gestion in ('Alto Valor','Masivo Gestionado'))
            ----and [player_id]=19795
            
            GROUP BY
            Cuenta,
            Casino,
            Casino_Cliente,
            Categoria_Tarjeta,
            Estado_Cliente,
            Segmento_Gestion,
            Segmento_Juego,
            Año,
            Mes""".format( fecha_inicial, fecha_final )

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df


    #############################
    def sqlquery_sesionesTGM(self, fecha_inicial, fecha_final, dictio_credenciales = 'env_dictio.json'):

        """Funcion para correr un SQL query y obtener informacion de sesiones en TGM. Query diseñada por Alain Alvarado.
        Args:
            fecha_inicial = fecha inicial del periodo a considerar
            fecha_final = fecha final del periodo a considerar

        Returns:
            Un dataframe con los resultados del query
        """
        
        print('Nota: antes de usar la funcion recuerde revisar que los filtros del query son los que usted necesita')

        
        fecha_inicial = str(fecha_inicial)
        fecha_final = str(fecha_final)
        
        
        sqlquery = """
            Declare @inicio as date, @fin as date, @Unidad as int, @Clientes as int        
             Set @inicio = '{}'
             Set @fin = '{}'
                    
                    
             Select 
               Convert(date, t.gamingDate) Jornada
               , t.timestamp
               , t.Tpo_Juego_seg
               , t.Referencia Maquina
               , ct.CATEGORIA_TARJETA
               , t.Player_id Id_Mcc
               , (t.Monto * fa.Factor_TGM) * po.Hold Win_TGM
               , t.GameCount Spins
               , t.Monto*1000/NULLIF(t.GameCount,0) Ap_Promedio
               , t.Monto*1000 CoinIn
              , mc.Casino
             From  
               [Imercado]..[vwf_mcc_tafd] t
               Join(
               select ID_MCC, CATEGORIA_TARJETA, ESTADO_CLIENTE, CARTERA_VIGENTE
               from [imercado]..[dm_bse_clientes]
               ) ct on ct.ID_MCC = t.Player_id
               Join [imercado]..[tafd_resumen] mc on mc.player_id = t.Player_id and t.gamingDate=mc.Gamingdate
               Join 
               [imercado]..[Payoff_Factor_Acumulacion] fa on t.Gamingdate between fa.Fecha_Inicio and Fecha_Fin and t.casino_id = Id_Unidad
               Join 
               [imercado]..[Payoff_Unidades] po on t.gamingDate = po.Jornada and t.Casino_id = po.Casino
                    
             Where  t.[Linea de Negocio] = 'Slot Games'
               --and (ct.CATEGORIA_TARJETA = 'Platinum' or ct.CATEGORIA_TARJETA = 'Gold' or ct.CATEGORIA_TARJETA = 'Silver')
               and t.AccountType_ID= 1
               and t.MovementType= 1
               and t.Canceled= 0
               and mc.Casino <> 'Mendoza'
               and ct.ESTADO_CLIENTE in ('Normal','Miembro de honor')
               and ct.CARTERA_VIGENTE = 'S'
               and abs(t.Amount) between 0 and 900000
               and (
               (YEAR(timeplayed)>1899 and YEAR(timestamp)>1899 and t.gamingDate < '2016-05-01')
               or 
               (t.gamingDate >= '2016-05-01')
               )
               and mc.Gamingdate between @inicio and @fin
        """.format(fecha_inicial, fecha_final)

        df = self.sql_imercado(sqlquery, task = "query", dictio_credenciales = 'env_dictio.json' )
        
        return df
