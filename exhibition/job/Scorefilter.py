
import pandas as pd
class Scorefilter():        # 수집된 데이터 key값 생성 및 정제
    def __init__(self,df):
        self.df = df

    def makekey(self):
        if df['공시연도'].dtype == int:
            df['key'] = df['정보공시 학교코드'] + "-" + df['공시연도']
            result = df['key']
        else:
            df['공시연도'] = pd.to_numeric(df['공시연도'] , errors = 'coerce')
            df = df.dropna(subset = ['공시연도'])
            df['공시연도'] = df['공시연도'].astype(int)
            df['공시연도'] = df['공시연도'].astype({'공시연도':'str'})
            df['key'] = df['정보공시 학교코드'] + "-" + df['공시연도']
            result = pd.DataFrame(df['key'])
        return result
    def concatkey(self):
        result = pd.concat([pd.DataFrame(df['key']) , df] , axis = 1)
        return result

func_list = [makekey(), concatkey]

def key (func_list , df):
      f1 = func_list[0]
      f2 = func_list[1]
      return f2(f1(df))

        
