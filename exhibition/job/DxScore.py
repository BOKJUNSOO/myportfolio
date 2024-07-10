import pandas as pd
import os
import numpy as np
import warnings
warnings.filterwarnings(action = "ignore")

# data load
file_path = "../data/*.csv"
df = pd.read_csv(file_path)

#preprocess
df = df.loc[df['전체학생수']!= 0 , :]

#1) 학교자치문화 평가 

var1 = ['key','지역']   #학교자치문화
c1 = df[var1]




