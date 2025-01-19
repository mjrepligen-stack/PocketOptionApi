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
        period = 60
        
        # Obtener datos históricos iniciales
        candles = api.get_historical_candles(asset, period, count_request=10)
        if candles and len(candles) > 0:
            candles_df = pd.DataFrame(candles)
            candles_df['date'] = pd.to_datetime(candles_df['time'], unit='s')
            candles_df.drop(columns=['time'], inplace=True)
            candles_df = candles_df[['date', 'open', 'high', 'low', 'close']]
            
            # Mostrar gráfico inicial
            chart.set(candles_df)
            chart.show(block=False)
        
        while True:
            try:
                # Obtener datos en tiempo real
                real_time_candles = api.get_stream_data(asset, period, limit=1)
                if real_time_candles and len(real_time_candles) > 0:
                    # Convertir a DataFrame
                    real_time_df = pd.DataFrame(real_time_candles)
                    real_time_df['date'] = pd.to_datetime(real_time_df['timestamp'], unit='s')
                    real_time_df.drop(columns=['timestamp'], inplace=True)
                    real_time_df = real_time_df[['date', 'open', 'high', 'low', 'close']]
                    
                    chart.update(real_time_df.iloc[-1])
                    
                    # Mostrar datos para debug
                    print("\nÚltima vela:")
                    print(real_time_df)

            except Exception as e:
                logging.error(f"Error: {e}")
            
            time.sleep(0.1)
        
    except Exception as e:
        logging.error(f"Error principal: {e}")
        
    except KeyboardInterrupt:
        logging.info("Interrupción del usuario")
            
if __name__ == "__main__":
    main()

