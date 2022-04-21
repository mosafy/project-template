from jesse.strategies import Strategy
import jesse.indicators as ta
from jesse import utils
import numpy as np



class Kava_bb_rsi(Strategy):
    
    
    def hyperparameters(self) -> list[dict]:
        return [
        {'name': 'bb_period', 'type': int, 'min': 10, 'max': 300, 'default': 100, 'step': 10},
        
        {'name': 'rsi_period', 'type': int, 'min': 5, 'max': 30, 'default': 14,  'step': 2},
        {'name': 'RSI_lookback', 'type': int, 'min': 5, 'max': 30, 'default': 10, 'step': 2},
        
        {'name': 'RSI_bull', 'type': int, 'min': 20, 'max': 90, 'default': 50, 'step': 5},
        {'name': 'RSI_bear', 'type': int, 'min': 20, 'max': 90, 'default': 50, 'step': 5},
        
        {'name': 'ATRX', 'type': int, 'min': 1, 'max': 20, 'default': 10, 'step': 1},
        {'name': 'ATR_period', 'type': int, 'min': 10, 'max': 50, 'default': 20, 'step': 1},
        
        {'name': 'Stop_loss', 'type': float, 'min': 0, 'max': 1, 'default': 0.1, 'step': 1},
        {'name': 'take_profit', 'type': float, 'min': 0, 'max': 1, 'default': 0.1, 'step': 1},

        
    ]

        
    
                
    @property
    def bb(self):
        # Bollinger bands using default parameters and close as source
        return ta.bollinger_bands(self.candles, period = self.hp["bb_period"],sequential=False,  matype = 0)
    
    @property
    def rsi(self):
        return ta.rsi(self.candles, self.hp["rsi_period"], sequential=True)
        
    
    @property
    def RSIBull(self):
        if np.any(self.rsi[-self.hp["RSI_lookback"]:-1] > self.hp["RSI_bull"]):
            return True
        return False


    @property
    def RSIBear(self):
        if np.any(self.rsi[-self.hp["RSI_lookback"]:-1] > self.hp["RSI_bear"]):
            return True
        return False
    
    @property
    def BBull(self):
        return self.open < self.bb.lowerband and self.close > self.bb.lowerband
    
    @property
    def BBear(self):
        return self.open > self.bb.upperband and self.close < self.bb.upperband
    
    @property
    def atr(self):
        return ta.atr(self.candles, sequential = False, period = self.hp["ATRX"])

    
    def should_long(self) -> bool:
        # Go long if candle closes above upperband
        return self.BBull and self.RSIBull

    def should_short(self) -> bool:
        return False

    def should_cancel(self) -> bool:
        return True

    def go_long(self): 
        # Open long position using entire balance
        if not self.position.is_long:
            qty = utils.size_to_qty(self.capital, self.price, fee_rate=self.fee_rate)
            self.buy = qty, self.price
            # self.stop_loss = [(qty,self.atr*self.hp["ATR_period"]),
            #                  (qty, self.price*0.9)]
            self.stop_loss = (qty,self.atr*self.hp["ATR_period"])
            self.take_profit = (qty, self.price*1.1)
            

    def go_short(self):
        pass

    def update_position(self):
        qty = self.position.qty
        new_atr = self.atr*self.hp["ATRX"]
        if self.is_long:
            self.stop_loss = (qty, new_atr)
            
        # Close the position when candle closes below middleband
        #This is quite wrong, we should Check the criteria in the TradeingView script
        if self.BBear and self.is_long:
            self.liquidate()
        
        # #10% stop loss
        if self.is_long and self.price < self.position.entry_price*0.9:
            self.liquidate()
            
            
            
            
