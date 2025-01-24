import logging
import pandas as pd
import time
from lightweight_charts import Chart
from pocketoptionapi.stable_api import PocketOption

# Configuración básica
logging.basicConfig(level=logging.INFO)
pd.set_option('display.float_format', lambda x: '%.5f' % x)

def main():
    try:
        # Crear gráfico
        chart = Chart(toolbox=True)
        chart.legend(visible=True, font_size=14)
        chart.precision(5)

        chart.candle_style(up_color='#32CD32', down_color='#FF0000')
        
        # Conectar API
        ssid = r"""42["auth",{"session":"YOUR_SESSION","isDemo":1,"uid":0,"platform":2}]"""
        api = PocketOption(ssid)
        api.connect()
        
        # Esperar conexión
        while not (api.check_connect() and api.is_time_synced()):
            continue
            
        # Configuración
        asset = "AUDCAD_otc"
        period = 10
        
        # # Iniciar streaming
        # stream_id = api.start_streaming(asset, period)
        # if not stream_id:
        #     raise Exception("Error al iniciar streaming")
        
        chart_seted = False
            
        while True:
            try:
                data = api.get_stream_data(asset, period, limit=100)
                if data and len(data) > 0:
                    # Crear DataFrame
                    data_df = pd.DataFrame(data)
                    data_df['date'] = pd.to_datetime(data_df['timestamp'], unit='s')
                    data_df.drop(columns=['timestamp'], inplace=True)
                    
                    # Reordenar columna
                    data_df = data_df[['date', 'open', 'high', 'low', 'close']]
                    
                    print("\nÚltimas velas:")
                    print(data_df)
                    
                    if not chart_seted:
                        chart.set(data_df)
                        chart.show(block=False)
                        chart_seted = True
                    else:
                        chart.update(data_df.iloc[-1])
                        #return
            except Exception as e:
                logging.error(f"Error: {e}")
            
            time.sleep(0.03)
        
    except Exception as e:
        logging.error(f"Error principal: {e}")

    except KeyboardInterrupt:
        logging.info("Interrupción del usuario")
    finally:
        if 'api' in locals():
            #api.stop_streaming(stream_id)
            pass
            
        #chart.close()
if __name__ == "__main__":
    main()

