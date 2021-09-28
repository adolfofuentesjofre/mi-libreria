import pytest
from mypythonlib import lib_clase
import os

def test_multiplicador_entero_positivo():
    assert lib_clase.multiplicador(2,2) == 4
    
def test_multiplicador_entero_negativo():
    assert lib_clase.multiplicador(-2,-2) == 4
    
def test_feriado():
    #Test de probar una funcion
    feri = lib_clase().feriado('2020-01-01')
    assert feri
    
def test_sqlquery():
    #Test de hacer un query
    df = lib_clase().sql_imercado("select top 10 * from [imercado]..[dm_bse_clientes]")
    assert len(df)>0
