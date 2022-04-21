from tracemalloc import start
import jesse.helpers as jh
from jesse import exceptions
from jesse.modes.import_candles_mode.drivers.interface import CandleExchange
import pandas as pd
import requests
from itertools import chain
import time
import numpy as np
import arrow

class Kraken(CandleExchange):
    
    def __init__(self) -> None:
        
        super().__init__(
            name='Kraken',
            count=1000,
            rate_limit_per_second=0.33,
            backup_exchange_class=None
        )
        self.count = 1000
        self.endpoint = "https://api.kraken.com/0/public/Trades"
        

    def get_starting_time(self, symbol: str) -> int:
        
        symbol = jh.dashless_symbol(symbol)
        symbol_payload = symbol.replace("-USDT", "USD")

        payload = {"pair":symbol_payload, 
                   "since": "0",
        } 
        
        response = requests.get(self.endpoint, params=payload)
        
        if response.status_code == 502:
            raise exceptions.ExchangeInMaintenance('ERROR: 502 Bad Gateway. Please try again later')

        # unsupported symbol
        if response.status_code == 400:
            raise ValueError(response.json()['msg'])

        if response.status_code != 200:
            raise Exception(response.content)

        data = response.json()
        index = list(data["result"].keys())[0]
        starting_timestamp = data["result"][index][0][2]
        
        return starting_timestamp + 60_000 * 1440


    
    def fetch(self, symbol: str, start_timestamp_ms: int) -> list:
        
        print("fetching function started")
        
        #Arrow returns everything in seconds so we need to convert to milliseconds arrow*1000
        #Kraken returns everything in nanoseconds so we need to convert to milliseconds kraken/1,000,000
        #jesse takes everything in milliseconds, convert accordingly
        #arrow_to_timestamp just x*1000
        
        start_arrow = arrow.get(start_timestamp_ms, locale="utc").floor('minute')
        start_timestamp_ms = arrow.get(start_timestamp_ms).int_timestamp*1000
        end_timestamp_ms = start_arrow.shift(minutes = self.count).int_timestamp*1000
        #symbo_pyload = jh.dashless_symbol(symbol)
        symbol_pyload = symbol.replace("-USDT", "USD")
        results_list = []
        results = []
        
        i= 0
        while  end_timestamp_ms > start_timestamp_ms:
            print(f"entering loop {i}")
            print(f"start_timestamp_ms: {start_timestamp_ms} end_timestamp_ms: {end_timestamp_ms}")

            payload = {"pair":symbol_pyload, 
                   "since": str(arrow.get(start_timestamp_ms).int_timestamp), #timestamp in seconds
            }    
            
            response = requests.get(self.endpoint, params=payload)               
            
            # Exchange In Maintenance
            if response.status_code == 502:
                raise exceptions.ExchangeInMaintenance('ERROR: 502 Bad Gateway. Please try again later')

            # unsupported symbol
            if response.status_code == 400:
                raise ValueError(response.json()['msg'])
            
            data = response.json()
            
            try:
                last_timestamp_ns = int(data["result"]["last"])// 1_000_000
                index = list(data["result"].keys())[0]
                chunk = data["result"][index]
                results.append(chunk)
                i = i + 1
            except KeyError:
                print("KeyError")
                break
            #print(chunk)
            #checking by the second
            if arrow.get(int(last_timestamp_ns)).int_timestamp == arrow.get(start_timestamp_ms).int_timestamp:
                "breaking loop"
                break
            elif len(chunk) < 300:
                "chunk is less than 300"
                break
            else:
                start_timestamp_ms = arrow.get(last_timestamp_ns).timestamp()*1000
                
        print(f"broke loop ", end_timestamp_ms > start_timestamp_ms, last_timestamp_ns, end_timestamp_ms, start_timestamp_ms) 
        
        results_list = list(chain(*results))
        #print(results_list)
        arry = np.array(results_list)
        #return arry
        arry = arry[:, 0:3] #remove characters
        arry = arry.astype(np.float32)
        #print(arry)
        lin_space = np.linspace(start_arrow.int_timestamp, start_arrow.shift(minutes = self.count).int_timestamp, self.count)
        #print(lin_space)
        index = np.searchsorted(arry[:,2], lin_space)
        arry_list = np.split(arry, index)
        arry_list = [x for x in arry_list if len(x) > 0]
        #return(arry_list)
        ohlcv = [{"timestamp": arrow.get(int(r[0,-1]), locale="utc").floor("minutes").int_timestamp*1000,
                  "open": r[0,0], 
                  "high":np.max(r[:,0]),
                  "low":np.min(r[0]),
                  "close":r[-1,0],
                  "volume":sum(r[:,1])} 
                 for r in arry_list
                 ]
        
        
        
        return[{
        'id': jh.generate_unique_id(),
        'symbol': symbol,
        'exchange': self.name,
        'timestamp': int(d["timestamp"]),
        'open': float(d["open"]),
        'close': float(d["close"]),
        'high': float(d["high"]),
        'low': float(d["low"]),
        'volume': float(d["volume"])
        } for d in ohlcv]
        
