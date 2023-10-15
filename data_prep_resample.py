import h5py
import pandas as pd
import hdf5plugin
from copy import deepcopy
import matplotlib.pyplot as plt
from datetime import datetime
AAA = 33
class Data:
    def __init__(self,file_path) -> None:
        """
        Parameters
        ----------
        filepath:文件路径
        """
        self.file_path = file_path
        with h5py.File(file_path, "r") as f:    
            self.dates = list(f.keys())

    def strtotime(self,date_string):
        """
        将字符串解析为datetime对象
        """
        format_str = "%Y%m%d%H%M%S%f"
        timestamp = datetime.strptime(date_string, format_str)
        return timestamp
    
    def date_revision(self,df: pd.DataFrame):
        """
        对源数据进行重命名和格式调整
        """
        change_lst = ['last', 'high', 'low','a1', 'a2', 'a3', 'a4', 'a5', 'b1', 'b2', 'b3', 'b4',
        'b5','prev_close']
        df['date'] = df['date'].astype(str)
        df['time'] = df['time'].astype(str)
        df['t'] = df['date']+df['time']
        df['t'] = df['t'].apply(lambda x: self.strtotime(x))
        for change in change_lst:
            df[change] = df[change].apply(lambda x:int(x)/10000)
        df.rename(columns={'last': 'close'}, inplace=True)
        df = df.set_index('t')
        df = df.drop_duplicates(subset = 'volume')
        return df[['close','high','low','volume']]
    
    def volume_resample(self,df:pd.DataFrame,volume_target = 4000):
        '''
        按成交量抽样。超过4000倍数序列时抄一次
        '''
        last_multiple_position = 1
        sample = pd.DataFrame(columns=['high', 'low', 'close', 'volume','open'])
        sample.index.name = 't'
        open_price,high,low,volume_past,close_past = None , 0 , float('inf') ,0, 0
        name = ''
        for index, row in df.iterrows():
            volume = row['volume']
            if volume > volume_target*last_multiple_position:
                series = pd.DataFrame({'high': high, 'low':low, 'close':close_past, 'volume':volume_past,'open':open_price},index = [name])
                sample = sample.append(series)
                open_price,high,low,close_past = [row['close']]*4
                volume_past = volume
                name = index
                last_multiple_position +=1 
            else:
                if open_price == None:
                    open_price = row['close']
                high = max(high,row['high'])
                low = min(low,row['low'])
                volume_past = volume
                close_past = row['close']
                name = index
        return sample
    
    def time_resample(self,df:pd.DataFrame,time=60):
        '''
        time:抽样时间间隔,单位s
        '''
        sample = pd.DataFrame(columns=['high', 'low', 'close', 'volume','open'])
        sample.index.name = 't'
        name = df.index[0]
        open_price,high,low,volume_past,close_past = None , 0 , float('inf') ,0, 0
        start = df.index[0]
        for i in range(len(df.index)):
            row = df.iloc[i]
            if (df.index[i]-start).seconds>time:
                series = pd.DataFrame({'high': high, 'low':low, 'close':close_past, 'volume':volume_past,'open':open_price},index = [name])
                sample = sample.append(series)
                open_price,high,low,close_past = [row['close']]*4
                volume_past = df['volume'].iloc[i]
                name = df.index[i]
                start = df.index[i]
            else:
                if open_price == None:
                    open_price = df['close'][i]
                high = max(high,row['high'])
                low = min(low,row['low'])
                volume_past = df['volume'].iloc[i]
                close_past = row['close']
        return sample
    
    def generate(self,type = 0):
        """
        type == 0:按照volume抽样
        type == 1:按照时间抽样
        """
        if type == 0 :
            data = pd.DataFrame(columns=['high', 'low', 'close', 'volume'])
            data.index.name = 't'
            for dataset_name in self.dates:
                raw_data = pd.read_hdf(self.file_path,dataset_name)
                new_data = self.volume_resample(self.date_revision(raw_data))
                data = data.append(new_data)
            data.to_csv(f'./data/volume_{self.file_path[2:-3]}.csv')
        else:
            data = pd.DataFrame(columns=['high', 'low', 'close', 'volume'])
            data.index.name = 't'
            with h5py.File(self.file_path, "r") as f:    
                dataset_names = list(f.keys())
            for dataset_name in self.dates:
                raw_data = pd.read_hdf(self.file_path,dataset_name)
                new_data = self.time_resample(self.date_revision(raw_data))
                data = data.append(new_data)
            data.to_csv(f'./data/time_{self.file_path[2:-3]}.csv')
        
        return