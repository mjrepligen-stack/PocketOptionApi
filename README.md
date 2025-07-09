# PocketOption API

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)

Una API no oficial para interactuar con PocketOption, implementada en Python con soporte para operaciones asíncronas.

## ✨ Características

- 🔒 Conexión WebSocket segura y estable
- 🔄 Manejo automático de reconexiones
- ⏰ Sincronización precisa del tiempo con el servidor
- 💼 Soporte para cuentas demo y reales
- ⚠️ Manejo robusto de errores
- 📝 Logging detallado de operaciones
- 📘 Tipado estático para mejor desarrollo

## 🚀 Instalación

```bash
pip install -r requirements.txt
```

## 📖 Uso Básico

```python
import asyncio
from pocketoptionapi import PocketOption

async def main():
    # Inicializar la API
    api = PocketOption(ssid="TU_SSID_AQUÍ")
    
    # Conectar
    await api.connect()
    
    # Verificar conexión
    if api.check_connect():
        print("Conectado exitosamente")
        
        # Obtener balance
        balance = await api.get_balance()
        print(f"Balance actual: {balance}")
        
        # Realizar una operación
        result = await api.buy(
            amount=100,
            active="EURUSD",
            direction="call",
            duration=60
        )
        
        if result:
            print("Operación exitosa")
    
    # Cerrar conexión
    await api.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## 🛠️ Funciones Principales

### Autenticación
| Función | Descripción |
|---------|-------------|
| `connect()` | Establece conexión con el servidor |
| `check_connect()` | Verifica el estado de la conexión |
| `close()` | Cierra la conexión |

### Trading
| Función | Descripción |
|---------|-------------|
| `buy()` | Realiza una operación de compra |
| `check_win()` | Verifica el resultado de una operación |
| `get_balance()` | Obtiene el balance actual |
| `get_candles()` | Obtiene datos históricos de velas |

### Datos de Mercado
| Función | Descripción |
|---------|-------------|
| `get_asset_info()` | Obtiene información de un activo |
| `get_all_assets()` | Lista todos los activos disponibles |
| `get_open_trades()` | Obtiene operaciones abiertas |

## 🔍 Manejo de Errores

```python
from pocketoptionapi.ws.client import WebSocketError, AuthenticationError, ConnectionError

async def main():
    try:
        api = PocketOption(ssid="TU_SSID_AQUÍ")
        await api.connect()
    except AuthenticationError:
        print("Error de autenticación: Verifica tu SSID")
    except ConnectionError:
        print("Error de conexión: Verifica tu conexión a internet")
    except WebSocketError as e:
        print(f"Error general del WebSocket: {e}")
```

## 📝 Configuración del Logging

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='pocketoption.log'
)
```

## 🎯 Eventos y Callbacks

```python
async def on_balance_update(balance):
    print(f"Nuevo balance: {balance}")

async def main():
    api = PocketOption(ssid="TU_SSID_AQUÍ")
    api.on_balance_update = on_balance_update
    await api.connect()
```

## 🔒 Consideraciones de Seguridad

- 🚫 Nunca compartas tu SSID
- 🔐 Usa variables de entorno para las credenciales
- 🔄 Mantén actualizada la librería
- 🛡️ Verifica los certificados SSL en producción

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Por favor, asegúrate de:

1. ✅ Seguir el estilo de código existente
2. 🧪 Agregar tests para nuevas funcionalidades
3. 📚 Actualizar la documentación
4. ♻️ Mantener la compatibilidad hacia atrás

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo [LICENSE](LICENSE) para más detalles.

## ⚠️ Descargo de Responsabilidad

Esta es una API no oficial. Úsala bajo tu propio riesgo. No nos hacemos responsables por pérdidas financieras.

## Activos Disponibles

La API soporta una amplia gama de activos, incluyendo:

- Forex: EURUSD, GBPUSD, USDJPY, etc.
- Criptomonedas: BTCUSD, ETHUSD, etc.
- Índices: SP500, NASUSD, etc.
- Materias primas: XAUUSD (Oro), XAGUSD (Plata), etc.
- Acciones: AAPL, GOOGL, etc.

Para ver la lista completa de activos y sus IDs, consulta `pocketoptionapi.constants.ACTIVES`.

## Períodos Disponibles

Los siguientes períodos (en segundos) están disponibles para las velas:

- 1s, 5s, 10s, 15s, 30s
- 1m (60s), 2m, 5m, 10m, 15m
- 30m, 1h, 2h, 4h, 8h, 12h
- 1d, 1w, 1M

## Manejo de Errores

La API incluye manejo de errores robusto:

```python
try:
    result = api.buy(amount=1, active="EURUSD", direction="call", duration=60)
except Exception as e:
    print(f"Error en la operación: {e}")
```

## Notas Importantes

1. Esta es una API no oficial y no está asociada con PocketOption.
2. Asegúrate de manejar adecuadamente las credenciales y no compartir tu SSID.
3. Implementa límites de tasa y manejo de errores apropiados en tu aplicación.
4. Las operaciones de trading implican riesgo financiero.

## Ejemplo de Bot con Machine Learning

Se incluye un script de ejemplo `examples/ml_trading_bot.py` que muestra
cómo entrenar un modelo sencillo de aprendizaje automático con datos
históricos y ejecutar operaciones automáticas. El código utiliza un
modelo de regresión logística y actualiza sus parámetros con cada vela
nueva.

> Este ejemplo es educativo y no garantiza ningún nivel de precisión ni
> ganancias.

## Contribuir

Las contribuciones son bienvenidas. Por favor, abre un issue o pull request en GitHub.
