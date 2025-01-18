import logging
import time
from datetime import datetime, timedelta, timezone


logger = logging.getLogger(__name__)

class TimeSynchronizer:
    def __init__(self):
        self.server_time_reference = None
        self.local_time_reference = None
        self.offset = None

    def synchronize(self, server_time):
        """
        Sincroniza el tiempo local con el tiempo del servidor.

        :param server_time: Tiempo del servidor en segundos (puede ser un timestamp).
        """
        self.server_time_reference = server_time
        self.local_time_reference = time.time()
        self.offset = self.local_time_reference - self.server_time_reference
        logger.debug("synchronize > Tiempo sincronizado con el servidor")


    def get_synced_time(self):
        """
        Obtiene el tiempo sincronizado basado en el tiempo actual del sistema.

        :return: Tiempo sincronizado en segundos.
        """
        if self.server_time_reference is None or self.local_time_reference is None:
            raise ValueError("El tiempo no ha sido sincronizado aún.")

        # Calcula la diferencia de tiempo desde la última sincronización
        elapsed_time = time.time() - self.local_time_reference
        # Calcula el tiempo sincronizado
        synced_time = self.server_time_reference + elapsed_time + self.offset
        return synced_time

    def get_synced_datetime(self):
        """
        Convierte el tiempo sincronizado a un objeto datetime en la zona horaria local.

        :return: Tiempo sincronizado como un objeto datetime.
        """
        synced_time_seconds = int(self.get_synced_time())
        # Convertir a datetime en UTC
        synced_datetime = datetime.fromtimestamp(synced_time_seconds, tz=timezone.utc)
        local_datetime = synced_datetime.astimezone()
        # No necesitamos ajustar la zona horaria ya que el servidor usa UTC
        return local_datetime

    def update_sync(self, new_server_time):
        """
        Actualiza la sincronización con un nuevo tiempo del servidor.

        :param new_server_time: Nuevo tiempo del servidor en segundos.
        """
        self.synchronize(new_server_time)
