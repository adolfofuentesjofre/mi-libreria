import pytest
from mypythonlib import lib_clase
import os

    
def test_feriado():
    #Test de probar una funcion
    feri = lib_clase().feriado('2021-01-01')
    assert feri==True
    
def test_diaantesdeferiado():
    #Test de probar una funcion
    feri = lib_clase().diaantesdeferiado('2020-12-31')
    assert feri==True

def test_load_data_slope():
    #Test de hacer un query
    df = lib_clase().load_data_slope()
    assert len(df)>0  
    
def test_load_data_edad():
    #Test de hacer un query
    df = lib_clase().load_data_edad()
    assert len(df)>0
    
def test_sqlquery():
    #Test de hacer un query
    df = lib_clase().sql_imercado("select top 10 * from [imercado]..[dm_bse_clientes]")
    assert len(df)>0

def test_sql_azure():
    #Test de hacer un query
    df = lib_clase().sql_azure("select top 10 * from [dw].[fact_Gaming_slot]")
    assert len(df)>0
  
def test_sql_azuresandbox():
    #Test de hacer un query
    df = lib_clase().sql_azuresandbox("select top 10 * from [dbo].[SEGMENTOS_VENDETTA]")
    assert len(df)>0

def test_percentil():
    #Test de calculo percentil
    lista = list(range(1,100))
    percentil=lib_clase.percentile(lista, 50)  
    assert percentil==50.0
    
def test_MAD():
    #Test de calculo percentil
    lista = list(range(1,101))
    mad=lib_clase.MAD(lista) 
    assert mad==37.065055462640046
    
def test_correct_cuarentena1():
    #Test de hacer un query
    df = lib_clase().correct_cuarentena("2020-01-01","2021-01-01")
    a=df[0]
    assert a=='2019-04-30'
     
def test_correct_cuarentena2():
    #Test de hacer un query
    df = lib_clase().correct_cuarentena("2021-04-01","2021-07-01")
    a=df[0]
    assert a=='2021-02-14'
    
def test_correct_cuarentena3():
    #Test de hacer un query
    df = lib_clase().correct_cuarentena("2020-07-01","2021-07-01")
    a=df[0]
    assert a=='2019-09-13'
 