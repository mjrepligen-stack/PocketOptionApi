import asyncio
from datetime import datetime, timedelta, timezone
from collections import deque
import websockets
from websockets.exceptions import WebSocketException
import json
import logging
import ssl
import time
import pocketoptionapi.global_value as global_value
from pocketoptionapi.constants import REGION
from pocketoptionapi.ws.objects.time_sync import TimeSynchronizer
from typing import Optional, Dict, Any, List
import backoff
import re  # Agregar al inicio del archivo con los otros imports

# Configuración mejorada del logging
logger = logging.getLogger(__name__)

# Evitar duplicación de handlers
# if not logger.handlers:
#     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     handler = logging.StreamHandler()
#     handler.setFormatter(formatter)
#     logger.addHandler(handler)
    
# # Establecer nivel de logging solo si no está configurado
# if logger.level == 0:  # 0 es el nivel NOTSET
#     logger.setLevel(logging.INFO)

class WebSocketError(Exception):
    """Clase base para errores de WebSocket."""
    pass

class AuthenticationError(WebSocketError):
    """Error durante la autenticación."""
    pass

class ConnectionError(WebSocketError):
    """Error de conexión."""
    pass

class MessageError(WebSocketError):
    """Error en el procesamiento de mensajes."""
    pass

async def process_message(message: str) -> None:
    """
    Procesa los mensajes recibidos del WebSocket.
    
    Args:
        message: El mensaje a procesar
    
    Raises:
        MessageError: Si hay un error procesando el mensaje
    """
    try:
        data = json.loads(message)
        logger.debug(f"Mensaje recibido: {data}")

        if isinstance(data, dict) and 'uid' in data:
            logger.debug(f"UID procesado: {data['uid']}")
        elif isinstance(data, list) and len(data) > 0:
            event_type = data[0]
            event_data = data[1] if len(data) > 1 else None
            logger.debug(f"Tipo de evento: {event_type}")

    except json.JSONDecodeError as e:
        raise MessageError(f"Error decodificando JSON: {e}")
    except Exception as e:
        raise MessageError(f"Error procesando mensaje: {e}")

class WebsocketClient:
    def __init__(self, api) -> None:
        """
        Inicializa el cliente WebSocket.

        Args:
            api: Instancia de la clase PocketOptionApi

        Nota:
            El SSID debe proporcionarse como una raw string (r"") para mantener los caracteres
            de escape. Por ejemplo:
            ssid = r'42["auth",{"session":"...","isDemo":0}]'
        """
        self.api = api
        self._initialize_state()
        self._parse_ssid()
        
        self.region = REGION()
        self.url: Optional[str] = None
        self.loop = asyncio.get_event_loop()
        self.connected = global_value.websocket_is_connected
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.base_delay = 1  # Delay base para reconexión exponencial

    def _initialize_state(self) -> None:
        """Inicializa las variables de estado."""
        self.wait_second_message = False
        self.successCloseOrder = False
        self.history_data_ready = False
        self.updateStream = False
        self.updateHistoryNew = False
        self.history_data = None
        self.message = None
        self.ssid = global_value.SSID
        self.websocket = None

    def _parse_ssid(self) -> None:
        """
        Parsea el SSID para extraer isDemo y session usando expresiones regulares.
        """
        try:
            # Buscar isDemo y session directamente
            is_demo_match = re.search(r'"isDemo":(\d+)', self.ssid)
            session_match = re.search(r'"session":"([^"]+)"', self.ssid)
            
            if not is_demo_match or not session_match:
                raise ValueError("No se encontró isDemo o session en el SSID")
                
            self.is_demo = bool(int(is_demo_match.group(1)))
            self.session = session_match.group(1)
            
            logger.debug(f"Cuenta tipo: {'DEMO' if self.is_demo else 'REAL'}")
            
        except Exception as e:
            logger.warning(f"Error parseando SSID, usando modo demo por defecto: {e}")
            self.is_demo = True

    @backoff.on_exception(
        backoff.expo,
        (WebSocketException, ConnectionError),
        max_tries=5,
        max_time=30
    )
    async def websocket_listener(self, ws):
        """
        Escucha mensajes del WebSocket con reconexión exponencial.
        
        Args:
            ws: Conexión WebSocket
        """
        try:
            async for message in ws:
                await self.on_message(message)
        except Exception as e:
            logger.error(f"Error en websocket_listener: {e}")
            global_value.websocket_is_connected = False
            raise

    async def send_ping(self, ws):
        """
        Envía pings periódicos para mantener la conexión viva.
        
        Args:
            ws: Conexión WebSocket
        """
        ping_interval = 20
        last_ping_time = time.time()
        
        while True:
            try:
                current_time = time.time()
                if current_time - last_ping_time >= ping_interval:
                    if not global_value.websocket_is_connected:
                        logger.warning("Conexión perdida, deteniendo ping")
                        break
                        
                    try:
                        await ws.send('42["ps"]')
                        last_ping_time = current_time
                        logger.debug("Ping enviado exitosamente")
                    except Exception as e:
                        logger.error(f"Error enviando ping: {e}")
                        global_value.websocket_is_connected = False
                        break
                        
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error en ciclo de ping: {e}")
                global_value.websocket_is_connected = False
                break

    async def wait_for_successauth(self, ws):
        try:
            start_time = time.time()
            while time.time() - start_time < 2:  # 2 segundos de timeout
                if global_value.websocket_is_connected:
                    return True
                await asyncio.sleep(0.1)
            return False
        except Exception as e:
            logger.error(f"Error en autenticación: {e}")
            return False

    async def connect(self):
        """
        Establece la conexión WebSocket con reconexión exponencial.
        """
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        try:
            await self.api.close("TRY CLOSE CONNECTION")
        except Exception as e:
            logger.debug(f"Error cerrando conexión previa: {e}")

        available_servers = self._filter_servers()
        if not available_servers:
            raise ConnectionError("No hay servidores disponibles para el tipo de cuenta")

        while not global_value.websocket_is_connected and self.reconnect_attempts < self.max_reconnect_attempts:
            for url in available_servers:
                if global_value.websocket_is_connected:
                    break

                try:
                    delay = min(self.base_delay * (2 ** self.reconnect_attempts), 10)
                    logger.info(f"Intento de conexión {self.reconnect_attempts + 1} a: {url} ({'REAL' if not self.is_demo else 'DEMO'})")
                    
                    async with websockets.connect(
                        url,
                        ssl=ssl_context,
                        extra_headers=self._get_headers(),
                        timeout=30,
                        ping_interval=None,  # Deshabilitamos el ping automático
                        ping_timeout=None,
                        close_timeout=5,
                        max_size=1000000,
                        compression=None
                    ) as ws:
                        self.websocket = ws
                        self.url = url

                        try:
                            # Esperar mensaje inicial
                            message = await ws.recv()
                            if not message.startswith('0{"sid":'):
                                raise ValueError("Mensaje inicial inválido")
                            
                            # Enviar mensaje 40
                            await ws.send('40')
                            
                            # Esperar respuesta con nuevo sid
                            message = await ws.recv()
                            if not message.startswith('40{"sid":'):
                                raise ValueError("Segundo mensaje inválido")
                            
                            # Enviar autenticación
                            logger.debug(f"Enviando SSID: {self.ssid}")
                            await ws.send(self.ssid)
                            
                            # Esperar respuesta de autenticación
                            message = await ws.recv()
                            if message == '41':
                                logger.error("Error de autenticación: Servidor rechazó las credenciales")
                                break
                            
                            # Iniciar tareas de escucha y ping
                            tasks = [
                                self.websocket_listener(ws),
                                self.send_ping(ws)
                            ]
                            
                            try:
                                done, pending = await asyncio.wait(
                                    tasks,
                                    return_when=asyncio.FIRST_COMPLETED
                                )
                                
                                for task in pending:
                                    task.cancel()
                                    try:
                                        await task
                                    except asyncio.CancelledError:
                                        pass
                                    
                                for task in done:
                                    exc = task.exception()
                                    if exc:
                                        logger.error(f"Error en tarea: {exc}")
                                        raise exc
                            
                            except Exception as e:
                                logger.error(f"Error en las tareas: {e}")
                                continue
                                
                        except asyncio.TimeoutError:
                            logger.warning(f"Timeout en handshake con {url}")
                            continue
                        except Exception as e:
                            logger.error(f"Error en el handshake inicial: {e}")
                            continue

                except Exception as e:
                    logger.warning(f"Error de conexión: {e}")
                    self.reconnect_attempts += 1
                    await asyncio.sleep(delay)
                    continue

            if not global_value.websocket_is_connected:
                logger.warning(f"No se pudo conectar a ningún servidor, reintentando en {delay} segundos...")
                await asyncio.sleep(delay)

        if self.reconnect_attempts >= self.max_reconnect_attempts:
            raise ConnectionError("Se alcanzó el máximo número de intentos de reconexión")

    def _filter_servers(self) -> List[str]:
        """
        Filtra los servidores disponibles según el tipo de cuenta.
        Prioriza los servidores más estables.
        
        Returns:
            Lista de URLs de servidores filtrados
        """
        available_servers = []
        preferred_servers = []
        
        for url in self.region.get_regions(True):
            if self.is_demo:
                if "demo-api" in url.lower():  # Priorizar servidores demo-api
                    preferred_servers.append(url)
                elif "demo" in url.lower() or "try" in url.lower():
                    available_servers.append(url)
            elif not self.is_demo and "demo" not in url.lower() and "try" not in url.lower():
                if "api" in url.lower():  # Priorizar servidores api principales
                    preferred_servers.append(url)
                else:
                    available_servers.append(url)
                    
        # Retornar primero los servidores preferidos
        return preferred_servers + available_servers

    def _get_headers(self) -> Dict[str, str]:
        """
        Obtiene los headers para la conexión WebSocket.
        Headers capturados de una conexión real exitosa.
        """
        return {
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Origin": "https://pocketoption.com",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "es-ES,es;q=0.9",
            "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits"
        }

    async def send_message(self, message: str) -> None:
        """
        Envía un mensaje a través del WebSocket.
        
        Args:
            message: Mensaje a enviar
            
        Raises:
            ConnectionError: Si no hay conexión activa
        """
        if not global_value.websocket_is_connected:
            raise ConnectionError("No hay conexión WebSocket activa")

        if message is not None:
            self.message = message
            try:
                await self.websocket.send(message)
                logger.debug(f"send_message > Mensaje enviado: {message}")
            except Exception as e:
                logger.error(f"send_message > Error enviando mensaje: {e}")
                raise ConnectionError(f"send_message > Error enviando mensaje: {e}")

    async def on_message(self, message: str) -> None:
        """
        Procesa los mensajes recibidos del WebSocket.
        
        Args:
            message: Mensaje recibido
        """
        if isinstance(message, bytes):
            message = message.decode('utf-8')
            try:
                message_data = json.loads(message)
                # await self._handle_binary_message(message_data)
            except json.JSONDecodeError:
                logger.warning(f"on_message > Error decodificando mensaje binario: {message}")
            return

        await self._handle_text_message(message)

    # async def _handle_binary_message(self, message_data: Dict[str, Any]) -> None:
    #     """
    #     Procesa mensajes binarios decodificados.
        
    #     Args:
    #         message_data: Datos del mensaje decodificado
    #     """
    #     if "time" in message_data:
    #         await self._handle_time_sync(message_data)
    #     elif "balance" in message_data:
    #         await self._handle_balance_update(message_data)
    #     elif "requestId" in message_data and message_data["requestId"] == 'buy':
    #         await self._handle_order_data(message_data)
    #     elif self.history_data_ready and "data" in message_data:
    #         await self._handle_history_data(message_data)
    #     elif self.updateStream and isinstance(message_data, list):
    #         await self._handle_stream_update(message_data)


    async def on_error(self, error):  # pylint: disable=unused-argument
        logger.error(error)
        global_value.websocket_error_reason = str(error)
        global_value.check_websocket_if_error = True

    async def on_close(self, error):  # pylint: disable=unused-argument
        logger.debug(error)
        # logger.warning(f"Websocket connection closed. Reason: {error}")
        global_value.websocket_is_connected = False

    async def _handle_balance_update(self, data: Dict[str, Any]) -> None:
        """
        Maneja las actualizaciones de balance.
        
        Args:
            data: Datos de balance
        """
        if "uid" in data:
            global_value.balance_id = data["uid"]
        global_value.balance = data["balance"]
        global_value.balance_type = data["isDemo"]
        logger.debug(f"Balance actualizado: {data}")

    async def _handle_order_data(self, data: Dict[str, Any]) -> None:
        """
        Maneja los datos de órdenes.
        
        Args:
            data: Datos de la orden
        """
        global_value.order_data = data
        logger.debug(f"Orden recibida: {data}")

    async def _handle_history_data(self, data: Dict[str, Any]) -> None:
        """
        Maneja los datos históricos.
        
        Args:
            data: Datos históricos
        """
        self.history_data_ready = False
        if "data" in data:
            self.api.history_data = data["data"]
            logger.info(f"Datos históricos recibidos: {data}")
            if hasattr(self.api, '_history_data_event'):
                self.api._history_data_event.set()
        else:
            logger.warning("Mensaje de datos históricos no tiene el campo 'data'")

    # async def _handle_stream_update(self, data: List[Any]) -> None:
    #     """
    #     Maneja las actualizaciones del stream.
        
    #     Args:
    #         data: Datos del stream
    #     """
    #     self.updateStream = False
    #     if len(data) > 0 and isinstance(data[0], list):
    #         stream_data = data[0]
    #         if len(stream_data) >= 3:
    #             asset, timestamp, price = stream_data
    #             logger.debug(f"Datos del stream: asset={asset}, timestamp={timestamp}, price={price}")
                
                
                
    #             if hasattr(self.api, 'current_asset') and asset == self.api.current_asset:
    #                 self.api.current_price = price
                    
    #                 logger.debug(f"Datos actualizados para {asset}: price={price}, timestamp={timestamp}")

    async def _handle_text_message(self, message: str) -> None:
        """
        Procesa mensajes de texto del WebSocket.
        
        Args:
            message: Mensaje de texto a procesar
        """
        if message.startswith('0{"sid":'):
            logger.debug("Recibido SID inicial")
            await self.websocket.send("40")
        elif message == "2":
            await self.websocket.send("3")
        elif message.startswith('40{"sid":'):
            logger.debug("Recibido segundo SID, enviando autenticación")
            await self.websocket.send(self.ssid)
        elif message.startswith('451-['):
            await self._handle_451_message(message)
        elif message.startswith("42") and "NotAuthorized" in message:
            await self._handle_unauthorized()

    async def _decode_binary_message(self, binary_data: bytes, msg_type: str) -> Any:
        """
        Decodifica y procesa mensajes binarios según su tipo.
        
        Args:
            binary_data: Datos binarios a decodificar
            msg_type: Tipo de mensaje (ej: updateStream, updateAssets, etc)
            
        Returns:
            Datos decodificados y procesados
        """
        try:
            # Decodificar datos binarios a texto
            decoded_data = binary_data.decode('utf-8')
            # Convertir a objeto Python
            data = json.loads(decoded_data)
            
            logger.debug(f"Mensaje binario decodificado ({msg_type}): {json.dumps(data)}")
            
            # Procesar según tipo de mensaje
            if msg_type == "updateStream":
                if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
                    stream_data = data[0]
                    if len(stream_data) >= 3:
                        asset, timestamp, price = stream_data
                        self.api.updateStream = True
                        # Sincronizar tiempo
                        self.api.time_sync.synchronize(int(timestamp))
                        
                        # Crear estructura de datos del stream
                        stream_update = {
                            "asset": asset,
                            "timestamp": timestamp,
                            "price": price
                        }
                        
                        # Enviar datos al API
                        await self.api.process_stream_update(stream_update)
                        
                        logger.debug(f"updateStream > Stream update: {stream_data}")
                        return stream_data
                    
            elif msg_type == "updateAssets":
                if isinstance(data, list):
                    assets_info = []
                    for asset_data in data:
                        if isinstance(asset_data, list) and len(asset_data) >= 2:
                            asset_info = {
                                "id": asset_data[0],
                                "symbol": asset_data[1],
                                "data": asset_data[2:] if len(asset_data) > 2 else []
                            }
                            assets_info.append(asset_info)
                            logger.debug(f"updateAssets > Assets actualizados: {asset_info}")
                    logger.debug(f"updateAssets > Assets actualizados: {len(assets_info)} activos")
                    return assets_info
                    
            elif msg_type == "successupdateBalance":
                if isinstance(data, dict):
                    await self._handle_balance_update(data)
                    logger.debug(f"successupdateBalance > Balance actualizado: {data.get('balance')} (Demo: {data.get('isDemo') if data.get('isDemo') else 'Real'})")
                    return data
                    
            elif msg_type == "updateHistoryNew":
                try:
                    # Marcar que estamos recibiendo datos históricos
                    stream_key = f"stream_{data.get('asset', '')}"
                    setattr(self.api, stream_key, True)
                    logger.debug(f"Marcador de stream establecido: {stream_key}")
                    
                    if isinstance(data, dict) and "asset" in data:
                        processed_data = {
                            "asset": data["asset"],
                            "period": data.get("period"),
                            "ticks": [],
                            "candles": []
                        }
                        
                        # Procesar ticks del historial
                        if "history" in data:
                            logger.debug(f"updateHistoryNew > Procesando {len(data['history'])} ticks históricos")
                            for tick in data["history"]:
                                if isinstance(tick, list) and len(tick) >= 2:
                                    processed_tick = {
                                        "asset": data["asset"],
                                        "timestamp": tick[0],
                                        "price": tick[1]
                                    }
                                    await self.api.process_stream_update(processed_tick)

                            logger.info(f"updateHistoryNew > Procesados {len(data['history'])} ticks históricos")
                        
                        # Procesar velas
                        if "candles" in data:
                            logger.debug(f"updateHistoryNew > Procesando {len(data['candles'])} velas")
                            for candle in data["candles"]:
                                if isinstance(candle, list) and len(candle) >= 5:
                                    processed_candle = {
                                        "timestamp": candle[0],
                                        "open": candle[1],
                                        "close": candle[2],
                                        "high": candle[3],
                                        "low": candle[4]
                                    }
                                    processed_data["candles"].append(processed_candle)

                        logger.debug(f"updateHistoryNew > Historial procesado para {data['asset']}: {len(processed_data['ticks'])} ticks, {len(processed_data['candles'])} velas")
                        return processed_data
                    else:
                        logger.warning(f"updateHistoryNew > Datos no válidos o sin asset: {data}")
                except Exception as e:
                    logger.error(f"Error procesando updateHistoryNew: {e}")
            
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"on_message > Error decodificando mensaje binario: {e}")
            logger.debug(f"Datos binarios raw: {binary_data}")
            return None
        except Exception as e:
            logger.error(f"on_message > Error procesando mensaje binario: {e}")
            return None

    async def _handle_451_message(self, message: str) -> None:
        """
        Maneja los mensajes que comienzan con 451.
        Formato: 451-["tipo",{"_placeholder":true,"num":0}]
        
        Args:
            message: Mensaje 451 a procesar
        """
        try:
            json_part = message.split("-", 1)[1]
            msg_data = json.loads(json_part)
            msg_type = msg_data[0]
            logger.debug(f"Tipo de mensaje 451: {msg_type}")
            
            # Esperar mensaje binario si hay placeholder
            if len(msg_data) > 1 and isinstance(msg_data[1], dict) and msg_data[1].get("_placeholder"):
                try:
                    binary_data = await asyncio.wait_for(self.websocket.recv(), timeout=5)
                    if isinstance(binary_data, bytes):
                        logger.debug(f"Recibido mensaje binario para {msg_type}")
                        decoded_data = await self._decode_binary_message(binary_data, msg_type)
                        
                        # Procesar datos decodificados según tipo
                        handlers = {
                            "successauth": self._handle_auth_success,
                            "updateAssets": self._handle_assets_update,
                            "successupdateBalance": self._handle_balance_success,
                            "successopenOrder": self._handle_open_order_success,
                            "updateClosedDeals": self._handle_closed_deals,
                            "successcloseOrder": self._handle_close_order_success,
                            "loadHistoryPeriod": self._handle_history_period,
                            "updateStream": self._handle_stream_success,
                            "updateHistoryNew": self._handle_history_new
                        }

                        if msg_type in handlers:
                            await handlers[msg_type](decoded_data)
                        else:
                            logger.warning(f"Tipo de mensaje 451 no manejado: {msg_type}")
                            
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout esperando datos binarios para {msg_type}")
                    return
                except Exception as e:
                    logger.error(f"Error recibiendo datos binarios: {e}")
                    return

        except Exception as e:
            logger.error(f"Error procesando mensaje 451: {e}")

    async def _handle_assets_update(self, msg_data: List[Any]) -> None:
        """
        Maneja la actualización de activos.
        
        Args:
            msg_data: Datos del mensaje
        """
        logger.debug("Recibida actualización de activos")
        # Aquí puedes procesar los datos binarios de activos si es necesario

    async def _handle_auth_success(self, msg_data: List[Any]) -> None:
        """
        Maneja la autenticación exitosa.
        
        Args:
            msg_data: Datos del mensaje
        """
        logger.info("Autenticación exitosa")
        await self.on_open()
        await self._send_post_auth_messages()

    async def _send_post_auth_messages(self) -> None:
        """Envía mensajes necesarios después de la autenticación."""
        messages = [
            '42["indicator/load"]',
            '42["favorite/load"]',
            '42["price-alert/load"]',
            '42["changeSymbol", {"asset": "AUDCAD_otc", "period": 60}]'
        ]
        for msg in messages:
            await self.websocket.send(msg)

    async def _handle_balance_success(self, _: List[Any]) -> None:
        """Maneja la actualización exitosa del balance."""
        global_value.balance_updated = True
        logger.debug("_handle_balance_success > Balance actualizado exitosamente")

    async def _handle_open_order_success(self, _: List[Any]) -> None:
        """Maneja la apertura exitosa de una orden."""
        global_value.result = True
        logger.debug("_handle_open_order_success > Orden abierta exitosamente")

    async def _handle_closed_deals(self, _: List[Any]) -> None:
        """Maneja las operaciones cerradas."""
        self._updateClosedDeals = True
        self.wait_second_message = True
        logger.debug("_handle_closed_deals > Enviando cambio de símbolo para sincronización")

    async def _handle_close_order_success(self, _: List[Any]) -> None:
        """Maneja el cierre exitoso de una orden."""
        self.successCloseOrder = True
        self.wait_second_message = True
        logger.debug("_handle_close_order_success > Orden cerrada exitosamente")

    async def _handle_history_period(self, _: List[Any]) -> None:
        """Maneja la carga del período histórico."""
        self.history_data_ready = True

    async def _handle_stream_success(self, _: List[Any]) -> None:
        """Maneja la actualización exitosa del stream."""
        self.updateStream = True
        logger.debug("_handle_stream_success > Stream actualizado")

    async def _handle_history_new(self, _: List[Any]) -> None:
        """Maneja la actualización del nuevo historial."""
        self.updateHistoryNew = True
        logger.debug("_handle_history_new > Nuevo historial disponible")

    async def _handle_unauthorized(self) -> None:
        """Maneja el caso de usuario no autorizado."""
        logger.error("Usuario no autorizado: Por favor, use un SSID válido")
        global_value.ssl_Mutual_exclusion = False
        global_value.websocket_is_connected = False
        await self.websocket.close()

    async def on_open(self) -> None:
        """Procesa la apertura del WebSocket."""
        logger.info(f"Conectado a: {self.url}")
        global_value.websocket_is_connected = True
