import logging
import pandas as pd
import time
from pocketoptionapi.stable_api import PocketOption
import json


logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
pd.set_option('display.float_format', lambda x: '%.5f' % x)

def process_order_result(result):
    """Callback para procesar el resultado de la orden"""

    logging.info(f"Resultado de la orden: {json.dumps(result, indent=4)}")
            
    return result

def main():
    try:
        # Conectar API
        ssid = r""""""
        api = PocketOption(ssid)
        api.connect()
        
        # Esperar conexión
        while not (api.check_connect() and api.is_time_synced()):
            continue
            
        # Configuración
        asset = "AUDCAD_otc"
        
        while True:
            try:
                server_time = api.get_server_datetime()
                if server_time.second == 0:
                    order_data, order_status = api.buy(amount=1, active=asset, direction="call", expiration=10)

                    if order_status:
                        # Iniciar la verificación asíncrona con callback
                        api.check_win(order_data.get("id"), callback=process_order_result)

                print(server_time.strftime("%Y-%m-%d %H:%M:%S"))

            except Exception as e:
                logging.error(f"Error: {e}")
            
            time.sleep(1)
        
    except Exception as e:
        logging.error(f"Error principal: {e}")
        
    except KeyboardInterrupt:
        logging.info("Interrupción del usuario")
            
if __name__ == "__main__":
    main()