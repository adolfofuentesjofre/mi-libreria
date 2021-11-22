# modelosenjoy
Libreria con las funciones y querys desarrollados por el equipo de Data Science de Enjoy.

### Installation
Se puede instalar usando pip y el wheel del modelo:
```
pip install <path-to-wheel>
```
Donde path-to-wheel es la ubicacion del wheel.

### Get started
Para utilizar cualquiera de los metodos de la libreria, primero hay que importar la clase:

```
from modelosenjoy import modelosenjoy_clase

# LLamar a la funcion "feriados" y verificar si una fecha es un feriado en Chile
modelosenjoy_clase().feriado('2020-01-01')

# Correr el modelo Vendetta
modelosenjoy_clase().modeloVendetta()

# Hacer un SQL query
modelosenjoy_clase().sql_imercado("select top 10 * from <nombre de tabla>")

# Hacer el SQL query para obtener la categoria tarjeta de clientes usando los puntos del año 2019
modelosenjoy_clase().sqlquery_categoriatarjeta('2019-01-01', '2020-01-01')
```

### SQL queries
Para hacer queries se requiere tener un diccionario en formato JSON con las credenciales
para conectarse al VPN en el mismo directorio donde uno esta trabajando, y estar conectado al VPN.
El nombre default del archivo es env_dictio.json, se puede determinar otro si se desea.

Ejemplo del formato del archivo JSON para imercado:

{"server_imercado": "123.45.67.89", 
"db_imercado": "nombre", 
"username_imercado": "nombre_usuario", 
"password_imercado": "password_usuario", 
"driver_imercado": "{nombre_driver}" }

### Listado de funciones
La clase modelosenjoy_clase tiene los siguientes metodos:

- correccion_fechas_semestrales
- correct_cuarentena
- diaantesdeferiado
- edad_rango
- edad_rango_vectorcliente
- feriado
- feriados_Chile
- modeloCLTV
- modeloFuga
- modeloReinversion
- modeloSilverGold
- modeloVendetta
- percentile
- probandoMultiprocess
- resumenModelos
- sql_imercado
- sql_azure
- sql_azuresandbox
- sql_crm
- sqlquery_Recencia
- sqlquery_canje
- sqlquery_categoriatarjeta
- sqlquery_descuentoNCCreinversion
- sqlquery_fechaactivacion
- sqlquery_fichapromoyFNN
- sqlquery_horariojuego_FDS
- sqlquery_numcasinosporcliente
- sqlquery_puntosyfechas
- sqlquery_reinversion
- sqlquery_sesionesTGM
- tablaVectorCliente
- timebetweenvisits_cuarentena

Para obtener una descripcion de cualquiera de estos metodos, por ejemplo, el metodo feriado:
```
help(modelosenjoy_clase.feriado)
```
