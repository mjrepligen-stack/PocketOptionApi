import asyncio
from datetime import datetime, timedelta, timezone

import websockets
import json
import logging
import ssl
import time

# Suponiendo la existencia de estos módulos basados en tu código original
#import pocketoptionapi.constants as OP_code
import pocketoptionapi.global_value as global_value
from pocketoptionapi.constants import REGION
from pocketoptionapi.ws.objects.timesync import TimeSync
from pocketoptionapi.ws.objects.time_sync import TimeSynchronizer

logger = logging.getLogger(__name__)

timesync = TimeSync()




async def process_message(message):
    try:
        data = json.loads(message)
        print(f"Received message: {data}")

        # Procesa el mensaje dependiendo del tipo
        if isinstance(data, dict) and 'uid' in data:
            uid = data['uid']
            print(f"UID: {uid}")
        elif isinstance(data, list) and len(data) > 0:
            event_type = data[0]
            event_data = data[1]
            print(f"Event type: {event_type}, Event data: {event_data}")
            # Aquí puedes añadir más lógica para manejar diferentes tipos de eventos

    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
    except KeyError as e:
        print(f"Key error: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")


class WebsocketClient(object):
    def __init__(self, api) -> None:
        """
        Inicializa el cliente WebSocket.

        :param api: Instancia de la clase PocketOptionApi
        """
        self.updateHistoryNew = None
        self.updateStream = None
        self.history_data_ready = None
        self.successCloseOrder = False
        self.api = api
        self.message = None
        self.ssid = global_value.SSID
        self.websocket = None
        
        # Detectar si es cuenta demo basado en el SSID
        try:
            ssid_data = json.loads(self.ssid.split("[", 1)[1].rsplit("]", 1)[0])
            if isinstance(ssid_data, list) and len(ssid_data) > 1:
                auth_data = ssid_data[1]
                self.is_demo = bool(auth_data.get("isDemo", 1))  # Por defecto demo si no se especifica
            else:
                self.is_demo = True
        except Exception as e:
            logger.warning(f"Error parseando SSID, usando modo demo por defecto: {e}")
            self.is_demo = True
            
        self.region = REGION()
        self.url = None
        self.loop = asyncio.get_event_loop()
        self.wait_second_message = False
        self._updateClosedDeals = False
        self.sync = TimeSynchronizer()
        self.connected = global_value.websocket_is_connected

    async def websocket_listener(self, ws):
        try:
            async for message in ws:
                await self.on_message(message)
        except Exception as e:
            logging.warning(f"Error occurred: {e}")
            global_value.websocket_is_connected = False

    async def on_open(self):
        """Method to process websocket open."""
        logger.info(f"Conectado a: {self.url}")
        global_value.websocket_is_connected = True

    async def send_ping(self, ws):
        """
        Envía pings periódicos para mantener la conexión viva.
        El servidor espera un ping cada 25 segundos, enviamos cada 20 para estar seguros.
        """
        ping_interval = 20  # segundos
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
                        
                await asyncio.sleep(1)  # Revisar cada segundo
                
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
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        try:
            await self.api.close("TRY CLOSE CONNECTION")
        except Exception as e:
            logger.debug(f"Error cerrando conexión previa: {e}")

        # Filtrar servidores basado en si es demo o real
        available_servers = []
        for url in self.region.get_regions(True):
            if self.is_demo and ("demo" in url.lower() or "try" in url.lower()):
                available_servers.append(url)
            elif not self.is_demo and "demo" not in url.lower() and "try" not in url.lower():
                available_servers.append(url)

        if not available_servers:
            logger.error("No hay servidores disponibles para el tipo de cuenta")
            return

        while not global_value.websocket_is_connected:
            for url in available_servers:
                if global_value.websocket_is_connected:
                    break

                logger.info(f"Intentando conectar a: {url} ({'DEMO' if self.is_demo else 'REAL'})")
                try:
                    async with websockets.connect(
                        url,
                        ssl=ssl_context,
                        extra_headers={
                            "Origin": "https://pocketoption.com",
                            "Cache-Control": "no-cache",
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                        },
                        timeout=3,
                        ping_interval=None,  # Desactivamos el ping automático de websockets
                        ping_timeout=None    # Usaremos nuestro propio sistema de ping
                    ) as ws:
                        self.websocket = ws
                        self.url = url

                        # Iniciar tareas
                        listener = asyncio.create_task(self.websocket_listener(ws))
                        ping = asyncio.create_task(self.send_ping(ws))
                        
                        try:
                            # Esperar a que alguna tarea termine (lo que indicaría un problema)
                            done, pending = await asyncio.wait(
                                [listener, ping],
                                return_when=asyncio.FIRST_COMPLETED
                            )
                            
                            # Cancelar las tareas pendientes
                            for task in pending:
                                task.cancel()
                            
                            # Si llegamos aquí, algo salió mal
                            logger.warning("Una de las tareas terminó inesperadamente")
                            if not global_value.websocket_is_connected:
                                continue  # Intentar con el siguiente servidor
                            
                        except Exception as e:
                            logger.error(f"Error en las tareas: {e}")
                            continue

                except Exception as e:
                    logger.warning(f"Error de conexión: {e}")
                    await asyncio.sleep(1)  # Esperar antes de intentar el siguiente servidor
                    continue

            if not global_value.websocket_is_connected:
                logger.warning("No se pudo conectar a ningún servidor, reintentando en 5 segundos...")
                await asyncio.sleep(5)

    async def send_message(self, message):
        """while global_value.websocket_is_connected is False:
            await asyncio.sleep(0.1)"""

        if global_value.websocket_is_connected and message is not None:
            self.message = message
            try:
                await self.websocket.send(message)
            except Exception as e:
                logger.warning(f"Error sending message: {e}")

    async def on_message(self, message):
        """Method to process websocket messages."""
        logger.debug(message)

        if type(message) is bytes:
            message = message.decode('utf-8')
            try:
                message = json.loads(message)
                logger.debug(f"Mensaje binario recibido: {message}")

                if "balance" in message:
                    if "uid" in message:
                        global_value.balance_id = message["uid"]
                    global_value.balance = message["balance"]
                    global_value.balance_type = message["isDemo"]

                elif "requestId" in message and message["requestId"] == 'buy':
                    global_value.order_data = message

                elif self.wait_second_message and isinstance(message, list):
                    self.wait_second_message = False
                    self._updateClosedDeals = False

                elif isinstance(message, dict) and self.successCloseOrder:
                    self.api.order_async = message
                    self.successCloseOrder = False

                elif self.history_data_ready and isinstance(message, dict):
                    self.history_data_ready = False
                    self.api.history_data = message["data"]

                elif self.updateStream and isinstance(message, list):
                    self.updateStream = False
                    self.api.time_sync.server_timestamp = message[0][1]
                    self.api.sync.synchronize(message[0][1])

                elif self.updateHistoryNew and isinstance(message, dict):
                    self.updateHistoryNew = False
                    self.api.historyNew = message

            except json.JSONDecodeError:
                logger.warning(f"Error decodificando mensaje binario: {message}")
            return

        # Manejo de mensajes de texto
        logger.debug(f"Mensaje de texto recibido: {message}")

        if message.startswith('0{"sid":'):
            logger.debug("Recibido SID inicial")
            await self.websocket.send("40")

        elif message == "2":
            await self.websocket.send("3")

        elif message.startswith('40{"sid":'):
            logger.debug("Recibido segundo SID, enviando autenticación")
            await self.websocket.send(self.ssid)

        elif message.startswith('451-['):
            try:
                json_part = message.split("-", 1)[1]
                msg_data = json.loads(json_part)

                if msg_data[0] == "successauth":
                    logger.info("Autenticación exitosa")
                    await self.on_open()
                    # Enviar mensajes post-autenticación
                    await self.websocket.send('42["indicator/load"]')
                    await self.websocket.send('42["favorite/load"]')
                    await self.websocket.send('42["price-alert/load"]')

                elif msg_data[0] == "successupdateBalance":
                    global_value.balance_updated = True
                elif msg_data[0] == "successopenOrder":
                    global_value.result = True
                elif msg_data[0] == "updateClosedDeals":
                    self._updateClosedDeals = True
                    self.wait_second_message = True
                    await self.websocket.send('42["changeSymbol",{"asset":"AUDNZD_otc","period":60}]')
                elif msg_data[0] == "successcloseOrder":
                    self.successCloseOrder = True
                    self.wait_second_message = True
                elif msg_data[0] == "loadHistoryPeriod":
                    self.history_data_ready = True
                elif msg_data[0] == "updateStream":
                    self.updateStream = True
                elif msg_data[0] == "updateHistoryNew":
                    self.updateHistoryNew = True

            except Exception as e:
                logger.error(f"Error procesando mensaje 451: {e}")

        elif message.startswith("42") and "NotAuthorized" in message:
            logger.error("Usuario no autorizado: Por favor, use un SSID válido")
            global_value.ssl_Mutual_exclusion = False
            global_value.websocket_is_connected = False
            await self.websocket.close()

    async def on_error(self, error):  # pylint: disable=unused-argument
        logger.error(error)
        global_value.websocket_error_reason = str(error)
        global_value.check_websocket_if_error = True

    async def on_close(self, error):  # pylint: disable=unused-argument
        logger.debug(error)
        # logger.warning(f"Websocket connection closed. Reason: {error}")
        global_value.websocket_is_connected = False
