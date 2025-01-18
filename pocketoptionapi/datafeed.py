"""
Módulo para manejar el streaming y almacenamiento de datos en tiempo real.
"""

import logging
import pandas as pd
from typing import Optional, Dict, Any, List
from datetime import datetime
import time
from collections import deque

logger = logging.getLogger(__name__)

class DataFeed:
    """
    Clase para manejar la obtención y procesamiento de datos históricos.
    """
    
    def __init__(self, api):
        """
        Inicializa el DataFeed.
        
        Args:
            api: Instancia de la API principal
        """
        self.api = api
        self._data_buffer = {}
        self._last_request_time = {}
        # Buffer para datos de stream en tiempo real
        self._stream_buffer = {}
        self._candle_buffer = {}  # {stream_id: {"current_candle": dict, "completed_candles": deque}}
        self._stream_buffer_size = 1500  # Máximo número de ticks por activo
        # Control de streams activos
        self._active_streams = {}  # {stream_id: {"asset": asset, "period": period, "active": True}}
        self._stream_counter = 0

    def generate_stream_id(self, asset: str) -> str:
        """
        Genera un ID único para un nuevo stream.
        """
        self._stream_counter += 1
        return f"stream_{self._stream_counter}_{asset}"

    def register_stream(self, asset: str, period: int) -> str:
        """
        Registra un nuevo stream y retorna su ID.
        
        Args:
            asset: Símbolo del activo
            period: Período en segundos
            
        Returns:
            str: ID único del stream
        """
        stream_id = self.generate_stream_id(asset)
        self._active_streams[stream_id] = {
            "asset": asset,
            "period": period,
            "active": True,
            "start_time": time.time()
        }
        return stream_id

    def deactivate_stream(self, stream_id: str) -> bool:
        """
        Desactiva un stream específico.
        
        Args:
            stream_id: ID del stream a desactivar
            
        Returns:
            bool: True si se desactivó correctamente
        """
        if stream_id in self._active_streams:
            self._active_streams[stream_id]["active"] = False
            return True
        return False

    def is_stream_active(self, stream_id: str) -> bool:
        """
        Verifica si un stream está activo.
        
        Args:
            stream_id: ID del stream
            
        Returns:
            bool: True si el stream está activo
        """
        return (stream_id in self._active_streams and 
                self._active_streams[stream_id]["active"])

    async def update_stream(self, asset: str, timestamp: float, price: float) -> None:
        """
        Actualiza el buffer de stream con nuevos datos y procesa las velas.
        Solo actualiza si hay algún stream activo para ese activo.
        
        Args:
            asset: Símbolo del activo
            timestamp: Timestamp del tick
            price: Precio del tick
        """
        try:
            # Obtener streams activos para este activo
            active_streams = [
                (stream_id, stream_info) 
                for stream_id, stream_info in self._active_streams.items()
                if stream_info["active"] and stream_info["asset"] == asset
            ]
            
            if not active_streams:
                return
                
            # Procesar el tick para cada stream activo del asset
            for stream_id, stream_info in active_streams:
                period = stream_info["period"]
                
                # Inicializar buffer de velas si no existe
                if stream_id not in self._candle_buffer:
                    self._candle_buffer[stream_id] = {
                        "current_candle": None,
                        "completed_candles": deque(maxlen=self._stream_buffer_size)
                    }
                
                candle_buffer = self._candle_buffer[stream_id]
                current_candle = candle_buffer["current_candle"]
                
                # Calcular el inicio del período actual
                current_period_start = (timestamp // period) * period
                
                # Si no hay vela actual, crear una nueva
                if current_candle is None:
                    candle_buffer["current_candle"] = self._init_candle(timestamp, price, period)
                    logger.debug(f"Nueva vela iniciada para {asset} en período {period}s")
                    continue
                
                # Si el tick pertenece a un nuevo período
                if timestamp >= current_candle["timestamp"] + period:
                    # Guardar la vela completada
                    candle_buffer["completed_candles"].append(current_candle)
                    logger.debug(f"Vela completada para {asset} en período {period}s")
                    # Iniciar nueva vela
                    candle_buffer["current_candle"] = self._init_candle(timestamp, price, period)
                else:
                    # Actualizar la vela actual
                    candle_buffer["current_candle"] = self._update_candle(current_candle, timestamp, price)
            
            logger.debug(f"Tick procesado para {asset}: timestamp={timestamp}, price={price}")
            
        except Exception as e:
            logger.error(f"Error actualizando stream buffer: {e}")
            logger.exception(e)

    def get_stream_data(self, stream_id: str, limit: int = None) -> List[dict]:
        """
        Obtiene las velas más recientes del stream para un ID específico.
        
        Args:
            stream_id: ID del stream
            limit: Número máximo de velas a retornar (None para todas)
            
        Returns:
            Lista de velas ordenadas por timestamp
        """
        try:
            if not self.is_stream_active(stream_id):
                return []
                
            if stream_id not in self._candle_buffer:
                return []
            
            # Obtener velas completadas y la vela actual
            candle_buffer = self._candle_buffer[stream_id]
            completed_candles = list(candle_buffer["completed_candles"])
            current_candle = candle_buffer["current_candle"]
            
            # Añadir la vela actual si existe
            if current_candle is not None:
                completed_candles.append(current_candle)
            
            # Ordenar por timestamp
            sorted_candles = sorted(completed_candles, key=lambda x: x["timestamp"])
            
            # Aplicar límite si se especifica
            if limit and limit > 0:
                sorted_candles = sorted_candles[-limit:]
            
            return sorted_candles
            
        except Exception as e:
            logger.error(f"Error obteniendo datos del stream: {e}")
            logger.exception(e)
            return []

    def get_data(self, active: str, period: int, start_time: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Obtiene datos históricos para un activo y período específicos.
        
        Args:
            active (str): Símbolo del activo
            period (int): Período en segundos
            start_time (int, optional): Timestamp de inicio
            
        Returns:
            Optional[pd.DataFrame]: DataFrame con los datos históricos o None si hay error
        """
        try:
            key = f"{active}_{period}"
            current_time = time.time()
            
            # Evitar solicitudes muy frecuentes para el mismo activo/período
            if key in self._last_request_time:
                time_since_last_request = current_time - self._last_request_time[key]
                if time_since_last_request < 1:  # Esperar al menos 1 segundo entre solicitudes
                    time.sleep(1 - time_since_last_request)
            
            self._last_request_time[key] = current_time
            
            # Solicitar datos históricos
            data = self._request_historical_data(active, period, start_time)
            if data is None:
                return None
                
            # Procesar y almacenar los datos
            df = self._process_data(data, period)
            if df is not None:
                self._data_buffer[key] = df
                
            return df
            
        except Exception as e:
            logger.error(f"Error obteniendo datos históricos: {e}")
            return None

    def _request_historical_data(self, active: str, period: int, start_time: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Realiza la solicitud de datos históricos al servidor.
        
        Args:
            active (str): Símbolo del activo
            period (int): Período en segundos
            start_time (int, optional): Timestamp de inicio
            
        Returns:
            Optional[Dict[str, Any]]: Datos históricos o None si hay error
        """
        try:
            # Usar el timestamp actual si no se proporciona uno
            if start_time is None:
                start_time = int(time.time())
            
            # Enviar solicitud al servidor
            self.api.websocket.send_message(
                f'42["loadHistoryPeriod",{{"asset":"{active}","index":{start_time},"period":{period}}}]'
            )
            
            # Esperar respuesta
            timeout = 10  # segundos
            start_wait = time.time()
            while time.time() - start_wait < timeout:
                if hasattr(self.api, 'history_data') and self.api.history_data is not None:
                    data = self.api.history_data
                    self.api.history_data = None
                    return data
                time.sleep(0.1)
                
            logger.error("Timeout esperando datos históricos")
            return None
            
        except Exception as e:
            logger.error(f"Error solicitando datos históricos: {e}")
            return None

    def _process_data(self, data: Dict[str, Any], period: int) -> Optional[pd.DataFrame]:
        """
        Procesa los datos históricos en un DataFrame.
        
        Args:
            data (Dict[str, Any]): Datos históricos crudos
            period (int): Período en segundos
            
        Returns:
            Optional[pd.DataFrame]: DataFrame procesado o None si hay error
        """
        try:
            if not isinstance(data, dict) or 'data' not in data:
                return None
                
            candles = data['data']
            if not candles:
                return None
                
            # Crear DataFrame
            df = pd.DataFrame(candles)
            
            # Convertir timestamps a datetime
            df['time'] = pd.to_datetime(df['time'], unit='s')
            
            # Ordenar por tiempo
            df = df.sort_values('time')
            
            # Establecer el índice
            df.set_index('time', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Error procesando datos históricos: {e}")
            return None

    def clear_buffer(self):
        """Limpia el buffer de datos."""
        self._data_buffer.clear()
        self._last_request_time.clear() 

    async def update_historical_ticks(self, historical_ticks: List[dict]) -> None:
        """
        Actualiza el buffer con un lote de ticks históricos.
        Los ticks deben estar ordenados por timestamp.
        
        Args:
            historical_ticks: Lista de diccionarios con los datos de los ticks
        """
        try:
            logger.info(f"update_historical_ticks > {historical_ticks}")
            if not historical_ticks:
                return
                
            # Agrupar ticks por asset
            ticks_by_asset = {}
            for tick in historical_ticks:
                asset = tick["asset"]
                if asset not in ticks_by_asset:
                    ticks_by_asset[asset] = []
                ticks_by_asset[asset].append({
                    "timestamp": tick["timestamp"],
                    "price": tick["price"]
                })
            
            # Procesar ticks para cada asset
            for asset, ticks in ticks_by_asset.items():
                # Verificar si hay algún stream activo para este asset
                is_active = any(
                    stream["active"] and stream["asset"] == asset 
                    for stream in self._active_streams.values()
                )
                
                if not is_active:
                    continue
                    
                # Ordenar ticks por timestamp
                sorted_ticks = sorted(ticks, key=lambda x: x["timestamp"])
                
                # Crear o actualizar el buffer para el asset
                if asset not in self._stream_buffer:
                    self._stream_buffer[asset] = deque(maxlen=self._stream_buffer_size)
                
                # Extender el buffer con los ticks ordenados
                self._stream_buffer[asset].extend(sorted_ticks)

                
                logger.info(f"Buffer histórico inicializado para {asset} con {len(sorted_ticks)} ticks")
                
        except Exception as e:
            logger.error(f"Error actualizando buffer histórico: {e}") 

    def _init_candle(self, timestamp: float, price: float, period: int) -> dict:
        """
        Inicializa una nueva vela con el primer tick.
        
        Args:
            timestamp: Timestamp del tick
            price: Precio del tick
            period: Período en segundos
            
        Returns:
            dict: Nueva vela inicializada
        """
        # Normalizar el timestamp al inicio del período
        candle_start = (timestamp // period) * period
        
        return {
            "timestamp": candle_start,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 1
        }

    def _update_candle(self, candle: dict, timestamp: float, price: float) -> dict:
        """
        Actualiza una vela existente con un nuevo tick.
        
        Args:
            candle: Vela actual
            timestamp: Timestamp del nuevo tick
            price: Precio del nuevo tick
            
        Returns:
            dict: Vela actualizada
        """
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["volume"] += 1
        return candle 