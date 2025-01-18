import random
import time
import logging
import pandas as pd
from datetime import datetime
from tzlocal import get_localzone

from pocketoptionapi.stable_api import PocketOption

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,  # Cambiar a DEBUG para ver más detalles
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)


# Configuración de pandas
pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# Obtener zona horaria local
local_tz = get_localzone()
print(f"Zona horaria local: {local_tz}")


def main():
    try:
        # SSID para autenticación (reemplazar con tu SSID)
        # Usamos r"" (raw string) para mantener los caracteres de escape tal cual
        # Esto es necesario porque el servidor espera recibir las comillas escapadas (\")
        ssid = r"""42["auth",{"session":"YOUR_SESSION","isDemo":1,"uid":0,"platform":2}]"""

        # Inicializar API
        api = PocketOption(ssid)
        api.connect()

        # Esperar a que la conexión esté lista
        connection_timeout = 10
        start_time = time.time()
        while not (api.check_connect() and api.is_time_synced()):
            if time.time() - start_time > connection_timeout:
                logging.error("Timeout esperando la conexión o sincronización")
                return
            time.sleep(0.5)
            
        logging.info("Conexión establecida y tiempo sincronizado")

        # Configuración de trading
        asset1 = "AUDCAD_otc"  # Primer activo
        period = 60  # Período en segundos

        balance = api.get_balance()
        print(f"Balance in test.py: {balance}")

        # Iniciar streaming para el primer activo
        stream_id1 = api.start_streaming(asset1, period)
        if not stream_id1:
            logging.error(f"Error iniciando streaming para {asset1}")
            return


        # Mantener el programa corriendo
        try:
            start_time = time.time()
            
            while True:
                server_datetime = api.get_server_datetime()
                print(f"\nHora del servidor: {server_datetime.strftime('%H:%M:%S')}")

                # Obtener datos del stream
                data1 = api.get_stream_data(stream_id1, limit=10)
                if data1:
                    print("\nÚltimos 10 ticks:")
                    print(pd.DataFrame(data1))
                
                time.sleep(1)  # Esperar 1 segundo entre actualizaciones

        except KeyboardInterrupt:
            logging.info("Deteniendo el programa...")
            api.stop_streaming(stream_id1)

    except Exception as e:
        logging.error(f"Error en la ejecución: {e}", exc_info=True)
    finally:
        logging.info("Programa finalizado")

if __name__ == "__main__":
    main()

