import pytest
from mypythonlib import lib_clase

def test_multiplicador_entero_positivo():
    assert lib_clase.multiplicador(2,2) == 4
    
def test_multiplicador_entero_negativo():
    assert lib_clase.multiplicador(-2,-2) == 4
