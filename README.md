# PocketOption API

Una API no oficial para interactuar con PocketOption, implementada en Python.

## Instalación

```bash
pip install -r requirements.txt
```

## Uso Básico

### Inicialización y Conexión

```python
from pocketoptionapi.stable_api import PocketOption

# Inicializar con SSID
ssid = r"""42["auth",{"session":"tu_session","isDemo":1,"uid":tu_uid,"platform":2}]"""
api = PocketOption(ssid)

# Conectar
success, error = api.connect()
if success:
    print("Conectado exitosamente")
else:
    print(f"Error al conectar: {error}")
```

### Obtener Datos de Velas (Candles)

```python
# Obtener velas de 10 segundos
candles = api.get_candles_full(
    active="AUDNZD_otc",  # Activo
    period=10,            # Período en segundos
    count=10             # Número de velas
)

if candles is not None:
    print("\nÚltima vela OHLC:")
    latest_candle = candles.iloc[-1]
    print(f"Tiempo: {latest_candle['time']}")
    print(f"Open:   {latest_candle['open']:.5f}")
    print(f"High:   {latest_candle['high']:.5f}")
    print(f"Low:    {latest_candle['low']:.5f}")
    print(f"Close:  {latest_candle['close']:.5f}")
```

### Realizar Operaciones de Trading

```python
# Realizar una compra
amount = 1          # Cantidad en dólares
asset = "AUDNZD_otc"  # Activo
action = "call"     # "call" para subida, "put" para bajada
expiration = 60     # Tiempo de expiración en segundos

success, order_id = api.buy(amount, asset, action, expiration)
if success:
    print(f"Orden ejecutada. ID: {order_id}")
else:
    print("Error ejecutando la orden")
```

### Verificar Resultados de Operaciones

Hay dos formas de verificar los resultados:

1. **Usando Callbacks**:
```python
def on_order_complete(result):
    print(f"\nOrden {result['id']} completada:")
    print(f"Estado: {result['status'].upper()}")
    print(f"Ganancia: {result['profit']}")
    print(f"Tiempo total: {result['execution_time']:.3f} segundos")
    print(f"Tiempo de apertura: {result['open_time']}")
    print(f"Tiempo de cierre: {result['close_time']}")

# Verificar resultado con callback
api.check_win(order_id, callback=on_order_complete)
```

2. **Consultando Resultados**:
```python
# Iniciar verificación
api.check_win(order_id)

# Más tarde, consultar resultado
result = api.get_order_result(order_id)
if result:
    print(f"Estado: {result['status']}")
    print(f"Ganancia: {result['profit']}")
```

## Ejemplo Completo

```python
import time
from pocketoptionapi.stable_api import PocketOption

def on_order_complete(result):
    print(f"\nOrden {result['id']} completada:")
    print(f"Estado: {result['status'].upper()}")
    print(f"Ganancia: {result['profit']}")
    print(f"Tiempo total: {result['execution_time']:.3f} segundos")
    print(f"Tiempo de apertura: {result['open_time']}")
    print(f"Tiempo de cierre: {result['close_time']}")

# Inicializar API
ssid = r"""42["auth",{"session":"tu_session","isDemo":1,"uid":tu_uid,"platform":2}]"""
api = PocketOption(ssid)

# Conectar
success, error = api.connect()
if not success:
    print(f"Error al conectar: {error}")
    exit(1)

# Esperar conexión
while not api.check_connect():
    time.sleep(1)

# Obtener velas
candles = api.get_candles_full("AUDNZD_otc", period=10, count=10)
if candles is not None:
    latest_candle = candles.iloc[-1]
    print(f"\nÚltimo precio: {latest_candle['close']}")

    # Realizar operación
    success, order_id = api.buy(1, "AUDNZD_otc", "call", 60)
    if success:
        # Verificar resultado
        api.check_win(order_id, callback=on_order_complete)
```

## Características Principales

- Conexión WebSocket para datos en tiempo real
- Soporte para cuenta demo y real
- Manejo asíncrono de verificación de resultados
- Conversión automática de zonas horarias
- Procesamiento de velas OHLC
- Sistema de callbacks para resultados de operaciones

## Períodos Disponibles

- Para velas < 60 segundos: máximo 2000 registros por petición
- Para velas >= 60 segundos: máximo 9000 registros por petición

Períodos soportados (en segundos):
```python
[1, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 43200, 86400, 604800, 2592000]
```

## Notas Importantes

1. El SSID es necesario para la autenticación y debe obtenerse de una sesión válida.
2. Las operaciones de verificación son asíncronas para no bloquear el código principal.
3. Los timestamps se convierten automáticamente a la zona horaria local.
4. Los resultados de las operaciones incluyen tiempos de apertura y cierre.
5. El sistema maneja automáticamente la reconexión en caso de pérdida de conexión.

## Manejo de Errores

La API incluye manejo de errores y logging para facilitar el diagnóstico de problemas:

```python
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

## Contribuciones

Las contribuciones son bienvenidas. Por favor, asegúrate de actualizar las pruebas según corresponda. 