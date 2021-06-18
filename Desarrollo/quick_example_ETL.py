# This is some API which provides around 50 different dataframes
import pydataxm as api

# libraries
import pandas as pd
import sqlite3
from datetime import datetime
from pandas.tseries.offsets import MonthEnd
import dateutil.relativedelta
from timeit import default_timer
from pathlib import Path
import os


end_date = datetime.now().date() #Se registra la fecha en que se hace el proceso


# ****************** Lectura de las hojas de excel que permiten parametrizar la ejecucion ***********************
# ***************************************************************************************************************

parametrizacion = pd.read_excel('parametrizacion.xlsx', sheet_name='dataframes') 
parametrizacion_columnas = pd.read_excel('parametrizacion.xlsx', sheet_name='columnas')

# Solo se tienen en cuenta las dataframes que indiquen que se les debe hacer el proceso de extracción dentro de la tabla parametrica
parametrizacion = parametrizacion[parametrizacion['Extraccion'] == 'si']




# *******  Inicializacion de los dataframes con sus respectivas estructuras *************************************
# ***************************************************************************************************************

analisis = pd.DataFrame(columns=['dataframe', 'columna', 'valor_minimo', 'valor_maximo', 'min=max',             # analisis -> DataFrame que contendra el analisis anivel de columnas
                                 'total', 'cantidad_nulos', 'porcentaje_nulidad', 'coeficiente_variacion'])


analisis_df = pd.DataFrame(columns=['variable', 'numero_registros', 'tiempo_ejecucion', 'exitoso', 'columnas']) # analisis_df -> analisis a nivel de tablas


analisis_global = pd.DataFrame(columns=['fecha_ejecucion', 'total_tablas_cargadas', 'total_tablas_no_cargadas', # analisis_global -> se crea un unico registro con el resumen de toda la ejecucion
                                        'porcentaje_exitoso', 'tiempo_total'])



# Se crea la conexion a la bd, aqui se especifica el nombre de la base de datos, si la bd no existe, automaticamente se crea 
# **************************************************************************************************************************

conexion = sqlite3.connect('bdprueba.db') # Db connection
cursor = conexion.cursor()
 

# ***** Inicializacion de las variables para el analisis global **************
# ****************************************************************************

tablas_cargadas = 0
tablas_off = 0
tablas_vacias = 0
inicio_global = default_timer()

#Fecha actual
now = datetime.now()

now_1 = str(now)[:10]
now_2 = str(now)[11:13]
now_3 = str(now)[14:16]

now = f"{now_1}-{now_2}-{now_3}" # fecha en formato "YYYY-MM-DD-HH-mm"



for index, registro in parametrizacion.iterrows():  # Iteración sobre la tabla parametrica donde se contienen los nombres de los Data Frames
    
    
    nombre_df = f"{registro[0]}"  #nombre del dataframe -> este mismo sera el nombre de la tabla en la bd
    nombre_df_off = f"off_{registro[0]}" #nombre del dataframe con el prefijo off -> este mismo sera el nombre de la tabla en la bd si esta no contiene las columnas que se esperan
    
    # registro[4] -> Número de mese a analizar
    
    apis = api.ReadDB() # se crea una instancia de la Api que me extrae los datos (se inicializa la API)
    initial_date = end_date - dateutil.relativedelta.relativedelta(months= registro[4])  # Se calcula el mes del cual se va a iniciar la extraccion de los datos
    print(end_date)
    print(initial_date)
    lista_columnas_viejas = parametrizacion_columnas.loc[(parametrizacion_columnas['dataframe'] == nombre_df), 'columna'].tolist() # Columnas que se desean en la extraccion

    
    inicio = default_timer()  # Se toma el tiempo de inicio de la extraccion de los datos
    df = apis.request_data(registro[2],registro[3],initial_date,end_date) # Se consume la API, se traen los datos y se almacenan en df
    fin = default_timer()  # se toma el tiempo fin de ejecucion

    df_original = df.copy() # se crea una copia del dataframe para que una vez se hagan los calculos necesarios se pueda guardar en la BD el dataframe original, incluyendo los valores nulos
    tiempo_ejecucion = fin - inicio  # la diferencia de estas variables me da el tiempo que tarde en extrae el dataframe (resultado en segundos)
    numero_registros = df.shape[0]  # Se almacena la cantidad de registros que tiene el dataframes (cantidad de filas)
   
      
    lista_columnas_nuevas = df.columns.to_list() # Nombre de las columnas que se obtuvieron en la extraccion
    
    
    columnas = df.columns # Se extraen las columnas que tiene el DF extraido y sobre las cuales se va a iterar
    
    df_columnas = columnas.to_list() # se crea una lista a aprtir de los nombres de las columnas
    
    if lista_columnas_viejas == lista_columnas_nuevas: # Se verifica que las columnas esperadas sean las mismas columnas que me trae la extraccion de los datos
        
        
        for columna in columnas: # Se itera sobre cada una de las columnas dentro de cada dataframe
            
        
            
            if parametrizacion_columnas.loc[(parametrizacion_columnas['dataframe'] == nombre_df) & (parametrizacion_columnas['columna'] == columna), 'remover_columna'].unique() == ['NO']:
            # antes de entrar en el analisis, se verifica si en la tabla parametrizacion se quiere eliminar o no la columna examinada. 
            # Si la respuesta es NO, se procede con el analisis, de lo contrario se elimina la columna seleccionada y se pasa a examinar la siguiente
            
                df[columna] = pd.to_numeric(df[columna], errors='ignore') # Las columnas que contienen numeros, se pasa a tipo numerica (en algunos casos pueden ser tipo String -> con esto se evita ese problema)
                num = df[columna].dtype # Se conptura el tipo de dato
                
                
                maximo = df[columna].max() # Se captura el valor maximo de la columna
                minimo = df[columna].min() # se captura el valor minimo de la columna
                
                # Las columnas van a ser renombradas, aqui se captura ese nuevo nombre que van a tener
                nueva_columna = parametrizacion_columnas.loc[(parametrizacion_columnas['dataframe'] == nombre_df) & (parametrizacion_columnas['columna'] == columna), 'nuevo_nombre_columna'].tolist()
                nueva_columna = nueva_columna[0]
                
                
                cantidad_nulos = df[columna].isnull().sum()  # Se calcula cuantos son los valores nulos que pasee cada columna
                registros_totales = df[columna].shape[0] # Recuento de la cantidad de registros a nivel de columna
                    
                porcentaje_nulidad = "{0:.2f}".format(cantidad_nulos/registros_totales) # Con los datos anteriores, se calcula el porcentaje de registros que son nulos 
                
                
                if minimo == maximo: # Se verifica si el valor maximo de cada columna es igual al menor
                    min_max = 'Verdadero'
                else:
                    min_max = 'Falso'
                
                
                if (num == 'float64') | (num == 'int64'): # Si la columna es de tipo numerica, se procede hacer un analisis mas,
                                                            # de lo contrario se pasa a almacenar el analisis tal cual como esta
                    df[columna] = pd.to_numeric(df[columna], errors='ignore')
                    df_original[columna] = pd.to_numeric(df[columna], errors='ignore')
                            
                    df[columna]  = df[columna] .fillna(0) # para efectos de los calculos, los valores que estan nulos se reemplazan con el '0'
                    total = "{0:.2f}".format(sum(df[columna])) # Se hace una suma de todos los registros, obteniendo asi un total al final por columana numerica
                    
                                       
                        
                    dv = df[columna].std() # desviacion estandar por columna
                    media = df[columna].mean() # promedio por columna
                    
                    coe_variacion = "{0:.2f}".format(dv/media) # coeficiente de variacion por columna
                    
                    analisis = analisis.append({'dataframe': nombre_df, 'columna': nueva_columna, 
                                                     'valor_minimo': minimo, 'valor_maximo': maximo, 
                                                     'min=max': min_max, 'total': total, 'cantidad_nulos': cantidad_nulos, 
                                                     'porcentaje_nulidad': porcentaje_nulidad, 'coeficiente_variacion': coe_variacion} , ignore_index=True)
                    
                    # posterior al analisis, se almacena en el dataframe 'analisis' el cual esta a nivel de columnas
                    
                else:  
                    
                    if columna == 'Date':

                        df[columna] = pd.to_datetime(df[columna], format="%Y-%m-%d")
                        df_original[columna] = pd.to_datetime(df_original[columna], format="%Y-%m-%d")
                        # primero se pasan las colunas a formato de fecha, pero por defecto crea el data con hora, minutos y segundos en cero
                        df[columna] = df[columna].apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))
                        df_original[columna] = df_original[columna].apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))
                        # se formatea de nuevo la fecha para que no tenga ni hora, ni segundo ni minuto
                        
                       
                        
                    analisis = analisis.append({'dataframe': nombre_df, 'columna': nueva_columna, 
                                     'valor_minimo': minimo, 'valor_maximo': maximo, 
                                     'min=max': min_max, 'total': 'No Aplica', 'cantidad_nulos': cantidad_nulos, 
                                     'porcentaje_nulidad': porcentaje_nulidad, 'coeficiente_variacion': "No Aplica"} , ignore_index=True)
                    
                    # si el campo no es numerico, se añade al dataframe 'analisis' indicando como 'No Aplica' los campos que se analizaron

                df.rename(columns={columna:nueva_columna}, inplace = True) # se renombra la columna con el nuevo nombre
                df_original.rename(columns={columna:nueva_columna}, inplace = True) # se renombra la columna con el nuevo nombre
                
    
            else:
                del df[columna] # se elimina la columna que se indica en la tabla parametrica
                del df_original[columna] # se elimina la columna que se indica en la tabla parametrica
            
            
            
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{nombre_df}'" #Consulta que permite verifica si la tabla ya existe en la bd
        df_comprobacion = pd.read_sql(query, conexion) #ejecucion de la consulta sobre la bd
        print(initial_date)
        if not df_comprobacion.empty: # Se verifica que el resultado no se vacio, de ser asi, nos indicaria que la tabla aun no ha sido creada
            # Si el resultado de la consulta no es vacio, se procede a actualizar los registros que correspondan, partiendo de los meses que se indican en la tabla parametrica
        
            consulta = (f"DELETE FROM '{nombre_df}' WHERE Date >= date('{initial_date}')")
            cursor.execute(consulta)
            conexion.commit() 
            
            df_original.to_sql(name=nombre_df, con= conexion, if_exists="append", index=False) # Save list of API variables in DB
            
        else:
            pass
            # Si el resultado es vacio, solo se agregan los nuevos registros
            df_original.to_sql(name=nombre_df, con= conexion, if_exists="append", index=False) # Save list of API variables in DB
                    
            
        df_columnas = df.columns.to_list() #lista de la columnas con sus nuevos nombres
        analisis_df = analisis_df.append({'variable': nombre_df, 'numero_registros': numero_registros, 
                                          'tiempo_ejecucion': tiempo_ejecucion, 'exitoso':'ok', 'columnas': df_columnas}, ignore_index=True)
        
        # Se añade al dataframe 'analisis_df' las caracteristicas que se identificaron
        
        tablas_cargadas = tablas_cargadas + 1 # conteo de las tablas que fuerin cargadas satisfactoriamente
        
        del df # Se elimina el df para liberar memoria
        print(f"{nombre_df}") # Se imprime el nombre del df que ha sido extraido y cargado satisfactoriamente, esto permite un rastreo por medio de la consola
        
        
    elif len(lista_columnas_nuevas) != 0: 
        # Si las columnas esperadas no son las que se reciben,  se hace un registro de la tabla con un prefijo 'off' que indica que la tabla no fue cargada
        
        analisis_df = analisis_df.append({'variable': nombre_df, 'numero_registros': numero_registros, 
                                          'tiempo_ejecucion': tiempo_ejecucion, 'exitoso':'off', 'columnas': df_columnas}, ignore_index=True)
        
        tablas_off = tablas_off + 1 # conteo de las tablas que no fueron cargadas en la bd
        
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{nombre_df_off}'"

        df_comprobacion = pd.read_sql(query, conexion)
        
        
        if not df_comprobacion.empty: # se verifica si existen registros para esa tabla, si existen, se procede a actualizar y agregar los respectivos registros nuevos
        
            consulta = (f"DELETE FROM '{nombre_df_off}' WHERE Date >= date('{initial_date}')")

            cursor.execute(consulta)
            conexion.commit()

            df_original.to_sql(name=nombre_df_off, con= conexion, if_exists="append", index=False) # Save list of API variables in DB
            # Se guarda en la bd con el nombre de 'nombre_df_off' para su posterior analisis
        else:
           
            df_original.to_sql(name=nombre_df_off, con= conexion, if_exists="append", index=False) # Save list of API variables in DB
            # Se guarda en la bd con el nombre de 'nombre_df_off' para su posterior analisis
        
    
    
    else: # si la tabla que se obtiene esta vacia, no se carga en la bd, pero se deja un registro en la hoja 'dataframe' indicando que la df estaba vacio desde su extraccion
        analisis_df = analisis_df.append({'variable': nombre_df, 'numero_registros': numero_registros, 
                                          'tiempo_ejecucion': tiempo_ejecucion, 'exitoso':'tabla_vacia'}, ignore_index=True)
        
        tablas_vacias = tablas_vacias +1 # conteo de las tablas estan vacias
        
conexion.close() #Se cierra la conexion a la base de datos


# ********** Caluclos para el analisis global ************
# ********************************************************

fin_global = default_timer()
tiempo_total_ejecucion = "{0:.2f}".format(fin_global - inicio_global) # tiempo total de la ejecucion, incluye tiempos de extraccion y analisis de las tablas

porcentaje_exitoso = "{0:.2f}".format(tablas_cargadas / (tablas_vacias + tablas_off + tablas_cargadas)) # porcentaje de tablas que fueron cargadas satisfactoriamente


analisis_global = analisis_global.append({'fecha_ejecucion': now, 'total_tablas_cargadas': tablas_cargadas, 
                                          'total_tablas_no_cargadas': (tablas_vacias + tablas_off), 
                                          'porcentaje_exitoso': porcentaje_exitoso, 'tiempo_total': tiempo_total_ejecucion}, ignore_index=True)

# se agregan los registros dentro del dataframe 'analisis_global' 



# ********* Creacion del archivo analisis con tres diferentes hojas
#              * datafame -> analisis a nivel de tablas
#              * columnas -> analisis a nivel de campos o columnas
#              * global -> analisis completo de la ejecucion


grabar = pd.ExcelWriter(f"{now}_analisis.xlsx", mode='w')
analisis_df.to_excel(grabar, sheet_name='dataframes', index= False)
analisis.to_excel(grabar, sheet_name='columnas', index= False)
analisis_global.to_excel(grabar, sheet_name='global', index= False)

grabar.save() # se cierra el guardado de datos en el archivo .xlsx







