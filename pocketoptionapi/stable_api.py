"""
PocketOption API Client

Este módulo proporciona una interfaz estable para interactuar con la API de PocketOption.
Implementa funcionalidades para trading, obtención de datos históricos y más.
"""

import asyncio
import threading
import time
import logging
import operator
from collections import defaultdict
from collections import deque
from datetime import datetime
from typing import Optional, Dict, List, Union, Tuple, Callable
from tzlocal import get_localzone
from queue import Queue
import pandas as pd

from pocketoptionapi.api import PocketOptionAPI
import pocketoptionapi.constants as OP_code
import pocketoptionapi.global_value as global_value
import json

# # Configuración del logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
logger = logging.getLogger(__name__)

# Obtener la zona horaria local del sistema
LOCAL_TIMEZONE = get_localzone()

def nested_dict(n, type):
    if n == 1:
        return defaultdict(type)
    else:
        return defaultdict(lambda: nested_dict(n - 1, type))

class PocketOption:
    """
    Cliente principal para interactuar con PocketOption.
    
    Esta clase proporciona una interfaz estable y fácil de usar para realizar
    operaciones de trading y obtener datos del mercado.
    
    Attributes:
        __version__ (str): Versión actual del cliente
        size (List[int]): Tamaños de velas disponibles en segundos
        suspend (float): Tiempo de suspensión entre operaciones
    """
    
    __version__ = "1.0.0"

    def __init__(self, ssid: str):
        """
        Inicializa el cliente de PocketOption.
        
        Args:
            ssid (str): ID de sesión para autenticación
        """
        # Configuración de tamaños de velas disponibles en segundos
        self.size = [
            1, 5, 10, 15, 30, 60, 120, 300, 600, 900,
            1800, 3600, 7200, 14400, 28800, 43200, 86400,
            604800, 2592000
        ]
        
        # Configuración básica
        self.suspend = 0.5
        self.thread = None
        
        # Configuración de sesión
        global_value.SSID = ssid
        self.SESSION_HEADER = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
            )
        }
        self.SESSION_COOKIE = {}
        
        # Inicialización de API y componentes
        self.api = PocketOptionAPI()
        #self.loop = asyncio.get_event_loop()
        
        # Inicializar DataFeed
        from pocketoptionapi.datafeed import DataFeed
        self.datafeed = DataFeed(self.api)

        # Estado de suscripciones
        self.subscribe_candle: List = []
        self.subscribe_candle_all_size: List = []
        self.subscribe_mood: List = []
        
        # Sistema de verificación de órdenes
        self._pending_orders = Queue()
        self._check_win_thread = threading.Thread(target=self._check_win_worker, daemon=True)
        self._check_win_thread.start()
        
        # Datos de trading
        self._init_trading_data()
        
        self._stream_thread = None
        self._stream_active = False

        

    def _init_trading_data(self):
        """Inicializa las estructuras de datos para trading."""
        self.get_digital_spot_profit_after_sale_data = nested_dict(2, int)
        self.get_realtime_strike_list_temp_data = {}
        self.get_realtime_strike_list_temp_expiration = 0

    # --------------------------------------------------------------------------

    def get_server_timestamp(self) -> int:
        """
        Obtiene el timestamp actual del servidor.
        
        Returns:
            int: Timestamp del servidor en segundos
        """
        return self.api.time_sync.get_synced_time()

    def get_server_datetime(self) -> datetime:
        """
        Obtiene la fecha y hora actual del servidor.
        
        Returns:
            datetime: Fecha y hora del servidor
        """
        return self.api.time_sync.get_synced_datetime()

    def set_session(self, header: Dict[str, str], cookie: Dict[str, str]) -> None:
        """
        Configura los headers y cookies de la sesión.
        
        Args:
            header: Headers HTTP personalizados
            cookie: Cookies de sesión
        """
        self.SESSION_HEADER = header
        self.SESSION_COOKIE = cookie

    async def _wait_for_connection(self, timeout: float = 30.0) -> Tuple[bool, Optional[str]]:
        """
        Espera a que se establezca la conexión WebSocket.
        
        Args:
            timeout: Tiempo máximo de espera en segundos
            
        Returns:
            Tuple[bool, Optional[str]]: (éxito de conexión, mensaje de error si hay)
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if global_value.check_websocket_if_error:
                return False, global_value.websocket_error_reason
            if global_value.websocket_is_connected:
                return True, None
            await asyncio.sleep(0.1)
        
        return False, "Timeout esperando la conexión WebSocket"

    async def _wait_for_timestamp(self, timeout: float = 10.0) -> bool:
        """
        Espera a que se sincronice el timestamp con el servidor.
        
        Args:
            timeout: Tiempo máximo de espera en segundos
            
        Returns:
            bool: True si se sincronizó correctamente, False si no
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.api.time_sync.server_timestamp is not None:
                return True
            await asyncio.sleep(0.1)
        return False

    async def connect_async(self) -> Tuple[bool, Optional[str]]:
        """
        Establece una conexión asíncrona con el servidor de PocketOption.
        
        Returns:
            Tuple[bool, Optional[str]]: (éxito de conexión, mensaje de error si hay)
        """
        try:
            # Reiniciar flags de conexión
            global_value.websocket_is_connected = False
            global_value.check_websocket_if_error = False
            global_value.websocket_error_reason = None
            global_value.ssl_Mutual_exclusion = False
            global_value.ssl_Mutual_exclusion_write = False
            
            # Iniciar conexión WebSocket
            await self.api.websocket.connect()
            
            # Esperar a que se establezca la conexión
            success, error = await self._wait_for_connection()
            if not success:
                return False, error or "Error de conexión WebSocket"
            
            # Esperar sincronización de timestamp
            if not await self._wait_for_timestamp():
                return False, "Error en sincronización de tiempo"
            
            return True, None
            
        except Exception as e:
            logger.error(f"Error durante la conexión: {str(e)}")
            return False, str(e)

    def connect(self) -> Tuple[bool, Optional[str]]:
        """
        Establece una conexión síncrona con el servidor de PocketOption.
        
        Returns:
            Tuple[bool, Optional[str]]: (éxito de conexión, mensaje de error si hay)
        """
        try:
            # Iniciar el hilo que manejará la conexión WebSocket
            self.websocket_thread = threading.Thread(
                target=self._run_websocket_thread,
                daemon=True
            )
            self.websocket_thread.start()
            
            # Esperar a que la conexión se establezca
            start_time = time.time()
            while time.time() - start_time < 30:  # 30 segundos de timeout
                if global_value.check_websocket_if_error:
                    return False, global_value.websocket_error_reason
                if global_value.websocket_is_connected:
                    return True, None
                time.sleep(0.1)
            
            return False, "Timeout en la conexión"
            
        except Exception as e:
            logger.error(f"Error en conexión: {str(e)}")
            return False, str(e)

    def _run_websocket_thread(self):
        """
        Ejecuta el bucle de eventos del WebSocket en un hilo separado.
        """
        try:
            # Crear y configurar el bucle de eventos para este hilo
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Reiniciar flags de conexión
            global_value.websocket_is_connected = False
            global_value.check_websocket_if_error = False
            global_value.websocket_error_reason = None
            global_value.ssl_Mutual_exclusion = False
            global_value.ssl_Mutual_exclusion_write = False
            
            # Ejecutar la conexión WebSocket
            loop.run_until_complete(self.api.websocket.connect())
            loop.run_forever()
            
        except Exception as e:
            logger.error(f"Error en el hilo WebSocket: {str(e)}")
            global_value.check_websocket_if_error = True
            global_value.websocket_error_reason = str(e)
        finally:
            loop.close()

    @staticmethod
    def check_connect() -> bool:
        """
        Verifica si hay una conexión activa con el servidor.
        
        Returns:
            bool: True si está conectado, False si no
        """
        return global_value.websocket_is_connected

    def get_balance(self) -> Optional[float]:
        """
        Obtiene el balance actual de la cuenta.
        
        Returns:
            Optional[float]: Balance actual o None si no está disponible
        """
        if global_value.balance_updated:
            return global_value.balance
        return None

    def get_async_order(self, buy_order_id: str) -> Optional[Dict]:
        """
        Obtiene información asíncrona de una orden.
        
        Args:
            buy_order_id: ID de la orden a consultar
            
        Returns:
            Optional[Dict]: Información de la orden o None si no se encuentra
        """
        try:
            if (self.api.order_async and 
                "deals" in self.api.order_async and 
                self.api.order_async["deals"] and 
                self.api.order_async["deals"][0]["id"] == buy_order_id):
                return self.api.order_async["deals"][0]
            return None
        except Exception as e:
            logger.debug(f"Error obteniendo orden asíncrona: {str(e)}")
            return None

    def get_async_order_id(self, buy_order_id):
        return self.api.order_async["deals"][0][buy_order_id]

    def start_async(self):
        asyncio.run(self.api.connect())

    def buy(self, amount: float, active: str, direction: str, expiration: int) -> Tuple[str, bool]:
        """
        Realiza una operación de compra.
        
        Args:
            amount: Cantidad a invertir
            active: Activo a operar
            direction: Dirección ('call' o 'put')
            expiration: Tiempo de expiración en segundos
            
        Returns:
            Tuple[str, bool]: (ID de la orden, Estado de la operación)
            - Si es exitosa: (ID de la orden, True)
            - Si falla: ('', False)
        """
        try:
            logger.debug(f"Iniciando compra: {direction} {amount} en {active}")

            request_id = direction + str(time.time())
            
            # Reiniciar variables de estado
            self.api._order_open_event.clear()
            global_value.order_data = None
            
            # Enviar orden de compra
            logger.debug("Enviando orden al servidor...")
            self.api.buyv3(amount, active, direction, expiration, request_id)
            
            # Esperar respuesta con timeout
            timeout = 1  # segundos
            
            if self.api._order_open_event.wait(timeout):
                if global_value.order_data.get("requestId") == request_id:
                    order_data = global_value.order_data
                    logger.debug(f"Orden ejecutada exitosamente. ID: {order_data}")
                    return order_data, True
                else:
                    error_msg = global_value.order_data.get("error", "Error desconocido") if isinstance(global_value.order_data, dict) else "Error en la orden"
                    logger.error(f"Error en la orden: {error_msg}")
                    return "", False
                        
            
            logger.error("Timeout esperando respuesta de la orden")
            return "", False
                
        except Exception as e:
            logger.error(f"Error ejecutando orden: {str(e)}", exc_info=True)
            return "", False

    def _check_win_internal(self, id_number: str, timeout: float = 120.0) -> Tuple[Optional[dict], str]:
        """
        Implementación interna de check_win.
        
        Returns:
            Tuple[Optional[dict], str]: (datos de la orden, estado)
            Los datos de la orden tienen el formato:
            {
                "deals": [{
                    "id": str,
                    "openTime": str,
                    "closeTime": str,
                    "openTimestamp": int,
                    "closeTimestamp": int,
                    "uid": int,
                    "amount": float,
                    "profit": float,
                    "percentProfit": float,
                    "percentLoss": float,
                    "openPrice": float,
                    "closePrice": float,
                    "command": int,
                    "asset": str,
                    "isDemo": int,
                    "currency": str,
                    "amountUSD": float
                }]
            }
        """
        try:
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    # Intentar obtener los datos del resultado almacenado
                    order_data = self.api._order_results.get(id_number)
                    if order_data and "deals" in order_data:
                        return order_data, "closed"
                        
                    # Si no hay datos almacenados, verificar la orden asíncrona
                    order_info = self.get_async_order(id_number)
                    if order_info:
                        return {"deals": [order_info]}, "pending"
                        
                except Exception as e:
                    logger.debug(f"Error temporal obteniendo orden: {str(e)}")
                    pass
                
                time.sleep(0.1)
            
            logger.error(f"Timeout verificando orden {id_number}")
            return None, "timeout"
            
        except Exception as e:
            logger.error(f"Error verificando orden {id_number}: {str(e)}")
            return None, "error"

    def _check_win_worker(self):
        """
        Hilo trabajador que procesa las órdenes pendientes y verifica sus resultados.
        """
        while True:
            try:
                # Obtener orden de la cola
                order_data = self._pending_orders.get()
                if order_data is None:  # Señal para terminar
                    break
                    
                order_id = order_data['id']
                start_time = order_data['time']
                callback = order_data.get('callback')
                
                # Verificar resultado
                result, status = self._check_win_internal(order_id)
                
                # Si la orden está pendiente y no ha expirado, volver a ponerla en la cola
                if status == "pending" and (time.time() - start_time) < 120:
                    self._pending_orders.put(order_data)
                    time.sleep(0.1)
                    continue
                
                
                # Llamar al callback si existe
                if callback and status != "pending":
                    try:
                        callback(result["deals"][0])
                    except Exception as e:
                        logger.error(f"Error en callback de orden {order_id}: {e}")
                    
            except Exception as e:
                logger.error(f"Error en worker de check_win: {e}")
            finally:
                self._pending_orders.task_done()

    def check_win(self, id_number: str, callback: Optional[Callable] = None) -> None:
        """
        Verifica el resultado de una operación de forma asíncrona.
        
        Args:
            id_number: ID de la orden a verificar
            callback: Función opcional que será llamada con el resultado
                     El callback recibirá un diccionario con:
                     - id: ID de la orden
                     - status: Estado de la orden ('closed', 'pending', 'timeout', 'error')
                     - deals: Datos completos de la orden del servidor (si está disponible)
                     - execution_time: Tiempo total de ejecución
        """
        self._pending_orders.put({
            'id': id_number,
            'time': time.time(),
            'callback': callback
        })

    def get_order_result(self, id_number: str) -> Optional[dict]:
        """
        Obtiene el resultado de una orden si está disponible.
        
        Args:
            id_number: ID de la orden
            
        Returns:
            Optional[dict]: Resultado de la orden con el formato del servidor o None si no está disponible
            El formato es:
            {
                "deals": [{
                    "id": str,
                    "openTime": str,
                    "closeTime": str,
                    "openTimestamp": int,
                    "closeTimestamp": int,
                    "uid": int,
                    "amount": float,
                    "profit": float,
                    "percentProfit": float,
                    "percentLoss": float,
                    "openPrice": float,
                    "closePrice": float,
                    "command": int,
                    "asset": str,
                    "isDemo": int,
                    "currency": str,
                    "amountUSD": float
                }]
            }
        """
        result = self.api._order_results.get(id_number)
        if result and "deals" in result and result["deals"]:
            return result
        return None
    
    def __del__(self):
        """Limpieza al destruir la instancia."""
        # Señalizar al hilo worker que termine
        if hasattr(self, '_pending_orders'):
            self._pending_orders.put(None)
            if hasattr(self, '_check_win_thread'):
                self._check_win_thread.join(timeout=1)

    def process_candles_data(self, data: Union[dict, list], period: int) -> Optional[pd.DataFrame]:
        """
        Procesa los datos de velas según el formato recibido.
        
        Args:
            data: Datos recibidos del servidor (puede ser dict o list)
            period: Período de las velas en segundos
            
        Returns:
            Optional[pd.DataFrame]: DataFrame procesado o None si hay error
        """
        try:
            # Si los datos vienen como lista, convertirlos al formato esperado
            if isinstance(data, list):
                data = {"data": data}
            
            if period < 60:
                return self._process_tick_data(data)
            else:
                return self._process_ohlc_data(data)
                    
        except Exception as e:
            logger.error(f"Error procesando datos de velas: {str(e)}")
            return None

    def _convert_to_local_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convierte la columna 'time' de timestamp a datetime en zona horaria local.
        Los timestamps de PocketOption vienen en UTC+2.
        """
        try:
            # Convertir timestamp a datetime (asumiendo que viene en UTC+2)
            df['datetime'] = pd.to_datetime(df['time'], unit='s')
            
            # Primero establecemos que el tiempo está en UTC+2
            df['datetime'] = df['datetime'].dt.tz_localize('Etc/GMT-2')  # El '-' es porque Etc/GMT usa convención opuesta
            
            # Convertir a zona horaria local
            df['datetime'] = df['datetime'].dt.tz_convert(str(LOCAL_TIMEZONE))
            
            # Mantener el timestamp original
            df['timestamp'] = df['time']
            
            # Reemplazar la columna time con el datetime local
            df['time'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Eliminar la columna datetime temporal
            df.drop('datetime', axis=1, inplace=True)
            
            # Asegurar que las columnas estén en el orden correcto
            cols = ['time', 'timestamp', 'open', 'high', 'low', 'close']
            for col in cols:
                if col not in df.columns:
                    logger.warning(f"Columna {col} no encontrada en el DataFrame")
            
            # Seleccionar solo las columnas que existen
            existing_cols = [col for col in cols if col in df.columns]
            df = df[existing_cols]
            
            return df
            
        except Exception as e:
            logger.error(f"Error convirtiendo timezone: {str(e)}")
            return df

    def _process_tick_list(self, data: list) -> pd.DataFrame:
        """
        Procesa lista de ticks y los convierte en velas OHLC.
        """
        try:
            # Convertir lista de diccionarios a DataFrame
            df = pd.DataFrame(data)
            
            # Convertir tiempo a timestamp entero
            df['time'] = df['time'].astype(float).astype(int)
            
            # Ordenar por tiempo
            df.sort_values('time', inplace=True)
            
            # Redondear el tiempo al período más cercano
            period = 10  # período fijo de 10 segundos
            df['period'] = (df['time'] // period) * period
            
            # Crear DataFrame OHLC
            result = []
            for period_time, group in df.groupby('period'):
                result.append({
                    'time': group['time'].iloc[0],
                    'open': group['price'].iloc[0],
                    'high': group['price'].max(),
                    'low': group['price'].min(),
                    'close': group['price'].iloc[-1]
                })
            
            # Convertir a DataFrame y ordenar
            ohlc = pd.DataFrame(result)
            ohlc.sort_values('time', inplace=True)
            ohlc.reset_index(drop=True, inplace=True)
            
            # Convertir timestamps a datetime local
            ohlc = self._convert_to_local_datetime(ohlc)
            
            return ohlc
            
        except Exception as e:
            logger.error(f"Error procesando ticks: {str(e)}")
            return pd.DataFrame()

    def _process_ohlc_list(self, data: list, period: int) -> pd.DataFrame:
        """
        Procesa lista de datos OHLC.
        """
        try:
            # Convertir lista de listas a DataFrame
            df = pd.DataFrame(data, columns=['time', 'close'])  # Cambiado de 'price' a 'close'
            
            # Convertir tiempo a timestamp entero
            df['time'] = df['time'].astype(float).astype(int)
            
            # Ordenar por tiempo
            df.sort_values('time', inplace=True)
            
            # Agrupar por período para crear velas OHLC
            df['period'] = df['time'] // period
            
            # Crear DataFrame OHLC
            result = []
            for period_time, group in df.groupby('period'):
                result.append({
                    'time': group['time'].iloc[0],
                    'open': group['close'].iloc[0],  # Primer precio del período
                    'high': group['close'].max(),
                    'low': group['close'].min(),
                    'close': group['close'].iloc[-1]  # Último precio del período
                })
            
            # Convertir a DataFrame y ordenar
            ohlc = pd.DataFrame(result)
            ohlc.sort_values('time', inplace=True)
            ohlc.reset_index(drop=True, inplace=True)
            
            # Convertir timestamps a datetime local
            ohlc = self._convert_to_local_datetime(ohlc)
            
            return ohlc
            
        except Exception as e:
            logger.error(f"Error procesando OHLC: {str(e)}")
            return pd.DataFrame()

    def _process_tick_data(self, data: dict) -> pd.DataFrame:
        """
        Procesa datos de ticks (períodos < 60 segundos).
        """
        try:
            # Obtener los datos, ya sea de data o directamente
            tick_data = data.get("data", data)
            if not tick_data:
                raise ValueError("Datos de ticks vacíos")
            
            # Convertir lista de diccionarios a DataFrame
            df = pd.DataFrame(tick_data)
            
            # Asegurar que tenemos las columnas necesarias
            if 'time' not in df.columns or 'price' not in df.columns:
                logger.error("Formato de datos inválido para ticks")
                return pd.DataFrame()
            
            # Convertir tiempo a timestamp entero
            df['time'] = df['time'].astype(float).astype(int)
            
            # Ordenar por tiempo
            df.sort_values('time', inplace=True)
            
            # Crear DataFrame OHLC
            ohlc = df.groupby(df['time']).agg({
                'price': ['first', 'max', 'min', 'last']
            }).reset_index()
            
            # Renombrar columnas
            ohlc.columns = ['time', 'open', 'high', 'low', 'close']
            
            # Convertir timestamps a datetime local
            ohlc = self._convert_to_local_datetime(ohlc)
            
            return ohlc
            
        except Exception as e:
            logger.error(f"Error procesando ticks: {str(e)}")
            return pd.DataFrame()

    def _process_ohlc_data(self, data: dict) -> pd.DataFrame:
        """
        Procesa datos OHLC (períodos >= 60 segundos).
        """
        try:
            # Obtener los datos, ya sea de data o directamente
            ohlc_data = data.get("data", data)
            if not ohlc_data:
                raise ValueError("Datos OHLC vacíos")
            
            # Crear DataFrame con los datos OHLC
            df = pd.DataFrame(ohlc_data)
            
            # Verificar columnas requeridas
            required_columns = ['time', 'open', 'high', 'low', 'close']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                # Si faltan columnas, intentar construir desde price
                if 'price' in df.columns:
                    df = df.rename(columns={'price': 'close'})
                    df['open'] = df['close']
                    df['high'] = df['close']
                    df['low'] = df['close']
                else:
                    raise ValueError(f"Faltan columnas requeridas: {missing_columns}")
            
            # Convertir tiempo a timestamp entero
            df['time'] = df['time'].astype(float).astype(int)
            
            # Ordenar por tiempo
            df.sort_values('time', inplace=True)
            df.reset_index(drop=True, inplace=True)
            
            # Convertir timestamps a datetime local
            df = self._convert_to_local_datetime(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error procesando OHLC: {str(e)}")
            return pd.DataFrame()

    # def get_candles_full(
    #     self, 
    #     active: str, 
    #     period: int, 
    #     start_time: Optional[int] = None,
    #     offset: Optional[int] = None,
    #     count_request: int = 1,
    # ) -> Optional[pd.DataFrame]:
    #     """
    #     Obtiene datos históricos de velas completos, combinando datos históricos con tiempo real.
    #     """
    #     try:
    #         logger.debug("Iniciando get_candles_full")
    #         # Obtener datos históricos del servidor
    #         historical_df = self._get_historical_candles(active, period, start_time, offset, count_request)
    #         logger.debug(f"Datos históricos obtenidos: {historical_df is not None}")
            
    #         # Combinar con datos en tiempo real del DataFeed
    #         if historical_df is not None:
    #             logger.debug("Combinando con datos en tiempo real")
    #             result = self.datafeed.merge_with_historical(active, period, historical_df)
    #             logger.debug(f"Combinación completada: {result is not None}")
    #             return result
            
    #         # Si no hay datos históricos, intentar usar solo datos en tiempo real
    #         logger.debug("Intentando obtener solo datos en tiempo real")
    #         return self.datafeed.get_data(active, period, start_time)
            
    #     except Exception as e:
    #         logger.error(f"Error obteniendo velas: {str(e)}", exc_info=True)
    #         return None

    def get_historical_candles(
        self, 
        active: str, 
        period: int, 
        start_time: Optional[int] = None,
        offset: Optional[int] = None,
        count_request: int = 1,
    ) -> Optional[list]:
        """
        Obtiene datos históricos del servidor.
        Para períodos >= 60s retorna lista de velas OHLC.
        Para períodos < 60s procesa ticks y los convierte a velas.
        Si start_time es None, rellena con datos recientes del buffer.
        """
        all_data = []
        buffer_data = None
        
        try:
            # Ajustar el offset según el período
            if offset is None:
                offset = 2000 if period < 60 else 9000
            else:
                offset = min(offset, 2000 if period < 60 else 9000)
            
            logger.debug(f"Usando offset={offset} para período de {period} segundos")
            
            # Determinar tiempo inicial
            current_server_time = self.api.time_sync.server_time_reference
            if start_time is None:
                time_start = (current_server_time // period) * period if period >= 60 else current_server_time
            else:
                time_start = int(start_time)
            
            current_time = time_start

        except Exception as e:
            logger.error(f"Error obteniendo datos: {str(e)}")
            return None

        try:
            # Para la primera iteración y períodos >= 60, intentar rellenar con datos del buffer
            if period >= 60 and start_time is None:
                # Buscar o iniciar stream
                stream_id = None
                stream_success = False
                
                # Primero buscar stream existente
                for sid, info in self.api.datafeed._active_streams.items():
                    if info["asset"] == active and info["period"] == period:
                        stream_id = sid
                        stream_success = True
                        logger.debug(f"Stream encontrado: {stream_id}")
                        break
                
                # Si no hay stream, intentar iniciarlo
                if stream_id is None:
                    logger.debug(f"Iniciando nuevo stream para {active} período {period}")
                    stream_success = self.start_streaming(active, period)
                    if stream_success:
                        # Esperar un momento para que lleguen datos iniciales
                        time.sleep(0.5)
                
                # Obtener datos del buffer si el stream está activo
                if stream_success:
                    buffer_data = self.api.datafeed.get_stream_data(active, period, limit=20)
                    if buffer_data:
                        logger.debug(f"Obtenidos {len(buffer_data)} datos del buffer")
                        
                        # Normalizar datos del buffer (timestamp -> time)
                        normalized_buffer = []
                        for candle in buffer_data:
                            try:
                                normalized_candle = candle.copy()
                                if "timestamp" in normalized_candle:
                                    normalized_candle["time"] = int(normalized_candle.pop("timestamp"))
                                    normalized_buffer.append(normalized_candle)
                                elif "time" in normalized_candle:
                                    normalized_candle["time"] = int(normalized_candle["time"])
                                    normalized_buffer.append(normalized_candle)
                            except Exception as e:
                                logger.error(f"Error normalizando vela: {e}")
                                continue
                        
                        if normalized_buffer:
                            # Filtrar velas completas (anteriores al tiempo actual)
                            current_candle_time = (current_server_time // period) * period
                            completed_candles = [
                                candle for candle in normalized_buffer 
                                if candle["time"] <= current_candle_time
                            ]
                            
                            if completed_candles:
                                latest_time = max(candle["time"] for candle in completed_candles)
                                logger.info(f"Agregando {len(completed_candles)} velas completas del buffer hasta {latest_time}")
                                all_data.extend(completed_candles)
                            else:
                                logger.debug("No hay velas completas en el buffer")
                    else:
                        logger.debug("Buffer vacío")
                else:
                    logger.warning("No se pudo iniciar el stream")

            # Realizar peticiones de datos históricos
            for i in range(count_request):
                if not self.check_connect():
                    logger.error("No hay conexión al servidor")
                    return None
                
                
                    
                # Limpiar datos anteriores y evento
                self.api.history_data = None
                self.api._history_data_event.clear()
                    
                # Enviar petición de velas
                self.api.getcandles(active, period, offset, int(current_time))
                    
                # Esperar respuesta usando Event
                if self.api._history_data_event.wait(timeout=10):
                    if self.api.history_data and "data" in self.api.history_data:
                        data = self.api.history_data["data"]
                            
                        if period < 60:
                            # Procesar ticks a velas
                            candles = self._process_ticks_to_candles(data, period)
                            if candles:
                                all_data.extend(candles)
                                logger.debug(f"Procesados {len(data)} ticks en {len(candles)} velas")
                                    
                                if i < count_request - 1:
                                    oldest_time = min(candle["time"] for candle in candles)
                                    current_time = oldest_time - period
                        else:
                            # Datos ya vienen en formato OHLC
                            if data:
                                all_data.extend(data)
                                logger.info(f"Obtenidas {len(data)} velas OHLC {max(candle['time'] for candle in all_data)}")
                                    
                                if i < count_request - 1:
                                    oldest_time = min(candle["time"] for candle in data)
                                    current_time = oldest_time - period
                            
                    else:
                        logger.warning("No se obtuvieron datos en esta iteración")
                else:
                    logger.error("Timeout esperando datos históricos")
                    
            
                if not all_data:
                    logger.error("No se obtuvieron datos")
                    return None
            
            # Asegurar que todas las velas tengan la clave "time"
            for candle in all_data:
                if "timestamp" in candle and "time" not in candle:
                    candle["time"] = candle.pop("timestamp")
            
            # Ordenar por tiempo y eliminar duplicados
            all_data.sort(key=lambda x: x.get("time", 0))
            seen = {}
            unique_data = []
            for candle in reversed(all_data):
                if candle.get("time") not in seen:
                    seen[candle.get("time")] = True
                    unique_data.append(candle)
            unique_data.reverse()
            
            logger.debug(f"Total de registros procesados: {len(unique_data)}")
            if unique_data:
                logger.debug(f"Rango de tiempo: {unique_data[0]['time']} a {unique_data[-1]['time']}")
            
            return unique_data
            
        except Exception as e:
            logger.error(f"Error procesando datos de velas: {str(e)}", exc_info=True)
            return None

    def _process_ticks_to_candles(self, ticks: list, period: int) -> list:
        """
        Procesa una lista de ticks y los convierte en velas OHLC.
        """
        if not ticks:
            return []
            
        candles = []
        current_candle = None
        
        for tick in sorted(ticks, key=lambda x: x["time"]):
            tick_time = tick["time"]
            candle_time = (tick_time // period) * period
            
            if current_candle is None or candle_time > current_candle["time"]:
                # Si hay una vela actual, guardarla
                if current_candle is not None:
                    candles.append(current_candle)
                
                # Iniciar nueva vela
                current_candle = {
                    "time": candle_time,
                    "open": tick["price"],
                    "high": tick["price"],
                    "low": tick["price"],
                    "close": tick["price"]
                }
            else:
                # Actualizar vela actual
                current_candle["high"] = max(current_candle["high"], tick["price"])
                current_candle["low"] = min(current_candle["low"], tick["price"])
                current_candle["close"] = tick["price"]
        
        # Agregar última vela si existe
        if current_candle is not None:
            candles.append(current_candle)
            
        return candles

    def process_30_second_data(self, df: pd.DataFrame, period: int) -> pd.DataFrame:
        """
        Procesa datos de velas de 30 segundos para convertirlos al período deseado.
        
        Args:
            df: DataFrame con datos de velas de 30 segundos
            period: Período objetivo en segundos
            
        Returns:
            pd.DataFrame: DataFrame procesado con el período objetivo
        """
        try:
            # Convertir tiempo a segundos y redondear
            df['time'] = df['time'].astype(float)
            df['time_30s'] = ((df['time'] // period) * period).astype(int)

            # Crear rango completo de tiempo
            start_time = df['time_30s'].min()
            end_time = df['time_30s'].max()
            all_times = pd.Series(
                range(start_time, end_time + period, period),
                name='time_30s'
            )

            # Asegurar que todos los intervalos estén presentes
            df = df.merge(all_times, on='time_30s', how='right')

            # Agrupar por intervalo y calcular OHLC
            grouped = df.groupby('time_30s').agg({
                'price': ['first', 'last', 'max', 'min']
            }).reset_index()

            # Renombrar columnas
            grouped.columns = ['time', 'open', 'close', 'high', 'low']

            return grouped
            
        except Exception as e:
            logger.error(f"Error procesando datos de 30 segundos: {str(e)}")
            raise

    @staticmethod
    def process_data_history(data, period):
        """
        Este método toma datos históricos, los convierte en un DataFrame de pandas, redondea los tiempos al minuto más cercano,
        y calcula los valores OHLC (Open, High, Low, Close) para cada minuto. Luego, convierte el resultado en un diccionario
        y lo devuelve.

        :param dict data: Datos históricos que incluyen marcas de tiempo y precios.
        :param int period: Periodo en minutos
        :return: Un diccionario que contiene los valores OHLC agrupados por minutos redondeados.
        """
        # Crear DataFrame
        df = pd.DataFrame(data['history'], columns=['timestamp', 'price'])
        # Convertir a datetime y redondear al minuto
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        # df['datetime'] = df['datetime'].dt.tz_convert(str(local_zone_name))
        df['minute_rounded'] = df['datetime'].dt.floor(f'{period / 60}min')

        # Calcular OHLC
        ohlcv = df.groupby('minute_rounded').agg(
            open=('price', 'first'),
            high=('price', 'max'),
            low=('price', 'min'),
            close=('price', 'last')
        ).reset_index()

        ohlcv['time'] = ohlcv['minute_rounded'].apply(lambda x: int(x.timestamp()))
        ohlcv = ohlcv.drop(columns='minute_rounded')

        ohlcv = ohlcv.iloc[:-1]

        ohlcv_dict = ohlcv.to_dict(orient='records')

        return ohlcv_dict

    @staticmethod
    def process_candle(candle_data, period):
        """
        Resumen: Este método estático de Python, denominado `process_candle`, toma datos de velas financieras y un período de tiempo específico como entrada.
        Realiza varias operaciones de limpieza y organización de datos utilizando pandas, incluyendo la ordenación por tiempo, eliminación de duplicados,
        y reindexación. Además, verifica si las diferencias de tiempo entre las entradas consecutivas son iguales al período especificado y retorna tanto el DataFrame procesado
        como un booleano indicando si todas las diferencias son iguales al período dado. Este método es útil para preparar y verificar la consistencia de los datos de velas financieras
        para análisis posteriores.

        Procesa los datos de las velas recibidos como entrada.
        Convierte los datos de entrada en un DataFrame de pandas, los ordena por tiempo de forma ascendente,
        elimina duplicados basados en la columna 'time', y reinicia el índice del DataFrame.
        Adicionalmente, verifica si las diferencias de tiempo entre las filas consecutivas son iguales al período especificado,
        asumiendo que el período está dado en segundos, e imprime si todas las diferencias son de 60 segundos.
        :param list candle_data: Datos de las velas a procesar.
        :param int period: El período de tiempo entre las velas, usado para la verificación de diferencias de tiempo.
        :return: DataFrame procesado con los datos de las velas.
        """
        # Convierte los datos en un DataFrame y los añade al DataFrame final
        data_df = pd.DataFrame(candle_data)
        # datos_completos = pd.concat([datos_completos, data_df], ignore_index=True)
        # Procesa los datos obtenidos
        data_df.sort_values(by='time', ascending=True, inplace=True)
        data_df.drop_duplicates(subset='time', keep="first", inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        data_df.ffill(inplace=True)
        if "symbol_id" in data_df.columns:
            data_df.drop(columns='symbol_id', inplace=True)
        # Verificación opcional: Comprueba si las diferencias son todas de 60 segundos (excepto el primer valor NaN)
        diferencias = data_df['time'].diff()
        diff = (diferencias[1:] == period).all()
        return data_df, diff

    def change_symbol(self, active, period):
        return self.api.change_symbol(active, period)

    def sync_datetime(self):
        return self.api.synced_datetime

    def sync_timestamp(self):
        return self.api.synced_timestamp

    def start_streaming(self, active: str, period: int = 60) -> Optional[str]:
        """
        Inicia el streaming de datos para un activo específico.
        
        Args:
            active: Nombre del activo
            period: Período en segundos (default: 60)
            
        Returns:
            Optional[str]: ID del stream si se inició correctamente, None si hubo error
        """
        try:
            # Verificar conexión
            if not self.check_connect():
                logger.error("No hay conexión al servidor")
                return None
            
            # Establecer el activo actual
            self.api.current_asset = active
            logger.debug(f"Activo actual establecido: {active}")
            
            # Registrar el stream en el DataFeed antes de cambiar el símbolo
            stream_id = self.api.datafeed.register_stream(active, period)
            
            # Enviar mensaje de cambio de símbolo
            self.api.change_symbol(active, period)
            logger.info(f"Iniciado streaming para {active} período {period}")
            
            # Esperar a que lleguen los primeros datos
            wait_start = time.time()
            stream_key = f"stream_{active}"
            
            while time.time() - wait_start < 5:  # Esperar máximo 5 segundos
                if hasattr(self.api, stream_key):
                    logger.info(f"Stream registrado con ID: {stream_id}")
                    return True if stream_id else False
                time.sleep(0.1)
            
            logger.warning("No se recibieron datos iniciales del stream")
            return None
            
        except Exception as e:
            logger.error(f"Error iniciando streaming: {str(e)}")
            return None

    def stop_streaming(self, stream_id: str) -> bool:
        """
        Detiene un stream específico.
        
        Args:
            stream_id: ID del stream a detener
            
        Returns:
            bool: True si se detuvo correctamente
        """
        try:
            # Desactivar el stream en el DataFeed
            if self.api.datafeed.deactivate_stream(stream_id):
                stream_info = self.api.datafeed._active_streams[stream_id]
                logger.info(f"Stream {stream_id} detenido para {stream_info['asset']}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error deteniendo stream: {str(e)}")
            return False

    def get_stream_data(self, asset: str, period: int, limit: int = 1) -> Optional[List[dict]]:
        """
        Obtiene los datos del stream.
        
        Args:
            stream_id: ID del stream
            limit: Número máximo de ticks a retornar
            
        Returns:
            Optional[List[dict]]: Lista de ticks o None si hay error
        """
        try:
            return self.api.datafeed.get_stream_data(asset, period, limit)
        except Exception as e:
            logger.error(f"Error obteniendo datos del stream: {str(e)}")
            return None

    def get_current_price(self) -> Optional[float]:
        """
        Obtiene el precio actual del stream.
        
        Returns:
            Optional[float]: Precio actual o None si no hay stream activo
        """
        return self.current_price if hasattr(self, 'current_price') else None

    def get_current_timestamp(self) -> Optional[float]:
        """
        Obtiene el timestamp actual del stream.
        
        Returns:
            Optional[float]: Timestamp actual o None si no hay stream activo
        """
        return self.current_timestamp if hasattr(self, 'current_timestamp') else None

    def process_history_new(self, active: str, period: int) -> None:
        """
        Procesa los datos de historyNew y los envía al DataFeed.
        """
        if self.api.historyNew:
            self.datafeed.update_from_history_new(active, period, self.api.historyNew)
            logger.info(f"Datos de historyNew procesados para {active}")
        else:
            logger.warning("No hay datos de historyNew disponibles")

    def get_ticks_data(self, active: str) -> pd.DataFrame:
        """
        Obtiene los datos de ticks almacenados en el DataFeed.
        
        Args:
            active: Nombre del activo
            
        Returns:
            DataFrame con los datos de ticks
        """
        return self.api.datafeed.get_ticks_data(active)

    def get_candles(self, active: str, period: int, start_time: Optional[int] = None, end_time: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Obtiene los datos de velas para un activo y período específicos.
        
        Args:
            active (str): El símbolo del activo
            period (int): Período de las velas en segundos
            start_time (Optional[int]): Timestamp inicial para los datos históricos
            end_time (Optional[int]): Timestamp final para los datos históricos
            
        Returns:
            Optional[pd.DataFrame]: DataFrame con los datos de las velas o None si hay error
        """
        try:
            logger.info(f"Solicitando velas para {active} período {period}s")
            
            # 1. Iniciar stream si no existe
            stream_id = None
            for sid, info in self.datafeed._active_streams.items():
                if info["active"] and info["asset"] == active and info["period"] == period:
                    stream_id = sid
                    break
                
            if not stream_id:
                logger.info("No hay stream activo, iniciando uno nuevo")
                stream_id = self.start_streaming(active, period)
                if not stream_id:
                    logger.error("No se pudo iniciar el stream")
                    return None
                
            # 2. Solicitar datos históricos
            historical_df = self._get_historical_candles(active, period, start_time)
            if historical_df is None:
                logger.warning("No se pudieron obtener datos históricos")
                return None
            
            # 3. Obtener datos recientes del buffer
            recent_data = self.datafeed.get_stream_data(stream_id)
            if not recent_data:
                logger.warning("No hay datos recientes en el buffer")
                return historical_df
            
            # 4. Verificar y completar datos faltantes
            if end_time:
                last_historical = historical_df[-1]["time"]
                first_recent = recent_data[0]["time"]
                
                if last_historical < first_recent:
                    logger.info(f"Hay un gap entre datos históricos y recientes: {last_historical} -> {first_recent}")
                    # Rellenar con datos del buffer si es posible
                    gap_data = [candle for candle in recent_data if last_historical < candle["time"] <= end_time]
                    if gap_data:
                        gap_df = pd.DataFrame(gap_data)
                        gap_df["time"] = pd.to_datetime(gap_df["time"], unit="s")
                        gap_df.set_index("time", inplace=True)
                        historical_df = pd.concat([historical_df, gap_df])
            
            # 5. Ordenar y eliminar duplicados
            historical_df = historical_df.sort_index()
            historical_df = historical_df[~historical_df.index.duplicated(keep="last")]
            
            return historical_df
            
        except Exception as e:
            logger.error(f"Error en get_candles: {e}")
            return None

    def is_time_synced(self) -> bool:
        """
        Verifica si el tiempo está sincronizado con el servidor.
        
        Returns:
            bool: True si el tiempo está sincronizado, False si no
        """
        try:
            return (self.api.time_sync.server_time_reference is not None and 
                   self.api.time_sync.local_time_reference is not None and 
                   self.api.time_sync.offset is not None)
        except Exception as e:
            logger.error(f"Error verificando sincronización de tiempo: {e}")
            return False
