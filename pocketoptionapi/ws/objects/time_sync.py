import logging
import time
from datetime import datetime, timezone, timedelta
from tzlocal import get_localzone

logger = logging.getLogger(__name__)

class TimeSynchronizer:
    def __init__(self):
        self.server_time_reference = None
        self.local_time_reference = None
        self.offset = None
        self._last_sync_check = 0
        self._sync_interval = 1
        self.server_timezone_offset = timedelta(hours=2)  # El servidor está 2 horas adelantado

    def synchronize(self, server_time):
        """
        Sincroniza el tiempo local con el tiempo del servidor.
        
        :param server_time: Tiempo del servidor en segundos
        """
        try:
            current_local = time.time()
            self.server_time_reference = float(server_time)
            self.local_time_reference = current_local
            # Ajustar el offset considerando la diferencia de zona horaria
            self.offset = current_local - (self.server_time_reference - self.server_timezone_offset.total_seconds())
            self._last_sync_check = current_local
            logger.debug(f"Tiempo sincronizado - Server: {self.server_time_reference}, Local: {self.local_time_reference}, Offset ajustado: {self.offset}")
        except Exception as e:
            logger.error(f"Error en synchronize: {str(e)}")

    def get_synced_time(self):
        """
        Obtiene el tiempo sincronizado actual.
        
        :return: Tiempo sincronizado en segundos
        """
        if not self.is_synced():
            logger.warning("Tiempo no sincronizado")
            return time.time()

        try:
            current_local = time.time()
            
            if current_local - self._last_sync_check > self._sync_interval:
                self._last_sync_check = current_local
                expected_server = self.server_time_reference + (current_local - self.local_time_reference)
                actual_local = current_local - self.offset
                drift = abs(expected_server - actual_local)
                
                if drift > 0.1:
                    logger.debug(f"Drift detectado: {drift}s")
                    self.offset = current_local - expected_server

            # Calcular tiempo sincronizado considerando la zona horaria del servidor
            synced_time = current_local - self.offset - self.server_timezone_offset.total_seconds()
            return synced_time
            
        except Exception as e:
            logger.error(f"Error en get_synced_time: {str(e)}")
            return time.time()

    def get_synced_datetime(self):
        """
        Obtiene el datetime actual sincronizado.
        
        :return: datetime sincronizado en zona horaria local
        """
        try:
            synced_time = self.get_synced_time()
            # Convertir a datetime considerando que el timestamp ya está ajustado
            local_tz = get_localzone()
            local_dt = datetime.fromtimestamp(synced_time, tz=local_tz)
            return local_dt
        except Exception as e:
            logger.error(f"Error en get_synced_datetime: {str(e)}")
            return datetime.now(get_localzone())

    def is_synced(self):
        """
        Verifica si el tiempo está sincronizado.
        
        :return: bool indicando si está sincronizado
        """
        return (self.server_time_reference is not None and 
                self.local_time_reference is not None and 
                self.offset is not None)

    def update_sync(self, new_server_time):
        """
        Actualiza la sincronización con nuevo tiempo del servidor.
        
        :param new_server_time: Nuevo tiempo del servidor en segundos
        """
        self.synchronize(new_server_time)
