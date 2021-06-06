from enum import auto
from numpy import append, float64, int64, intp, source
import pandas as pd
from pandas.io.formats.format import EastAsianTextAdjustment
import sqlalchemy
import psycopg2
import sqlite3
import time
import itertools
import numpy
from psycopg2.extensions import register_adapter, AsIs

def CrearHtml():
    pf = pd.read_csv("MedidasPLC2.csv",sep=";")
    html = pf.to_html()
    text_file = open("index.html","w")
    text_file.write(html)
    text_file.close()

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
register_adapter(numpy.float64, addapt_numpy_float64)

df = pd.read_csv("MedidasPLC2.csv",sep=";")#ABRIR CSV
def AgregarLista(lista,a,i):
        if(str(df.loc[i,a])=="No"):
            lista.append(i)

#REEMPLAZO DE ESPACIOS VACIOS O NULL
##df["Data/time"].fillna("No",inplace=True) 
##df["Data/time"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Anemo Top 104;wind_speed [m/s]"].fillna("No",inplace=True) 
df["Anemo Top 104;wind_speed [m/s]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Anemo 100;wind_speed [m/s]"].fillna("No",inplace=True) 
df["Anemo 100;wind_speed [m/s]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Anemo 90;wind_speed [m/s]"].fillna("No",inplace=True) 
df["Anemo 90;wind_speed [m/s]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Anemo 60;wind_speed [m/s]"].fillna("No",inplace=True) 
df["Anemo 60;wind_speed [m/s]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Anemo 30;wind_speed [m/s]"].fillna("No",inplace=True) 
df["Anemo 30;wind_speed [m/s]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Veleta 80;wind_direction [A]"].fillna("No",inplace=True) 
df["Veleta 80;wind_direction [A]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Veleta 40;wind_direction [A]"].fillna("No",inplace=True) 
df["Veleta 40;wind_direction [A]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Hygro/Thermo;temperature [A]"].fillna("No",inplace=True) 
df["Hygro/Thermo;temperature [A]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Barometro;air_pressure [mbar]"].fillna("No",inplace=True) 
df["Barometro;air_pressure [mbar]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["Humidity;voltage [% rel]"].fillna("No",inplace=True) 
df["Humidity;voltage [% rel]"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["A1"].fillna("No",inplace=True) 
df["A1"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["A2"].fillna("No",inplace=True) 
df["A2"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["A3"].fillna("No",inplace=True) 
df["A3"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["C1"].fillna("No",inplace=True) 
df["C1"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["C2"].fillna("No",inplace=True) 
df["C2"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["C3"].fillna("No",inplace=True) 
df["C3"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["C4"].fillna("No",inplace=True) 
df["C4"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["C5"].fillna("No",inplace=True) 
df["C5"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["D1"].fillna("No",inplace=True) 
df["D1"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["D2"].fillna("No",inplace=True) 
df["D2"].replace({"0":"No","1":"Si","null":"No"},inplace=True)
df["V"].fillna("No",inplace=True) 
df["V"].replace({"0":"No","1":"Si","null":"No"},inplace=True)

list_of_index = []
sumAT104 = 0
sumAT100 = 0
sumAT90 = 0
sumAT60 = 0
sumAT30 = 0
sumVel80 = 0
sumVel40 = 0
sumHygro = 0
sumBar = 0
sumHum = 0

for i in df.index:
    #AGREGANDO INDEX DE FILAS QUE LE FALTAN DATOS 
    AgregarLista(list_of_index,"Anemo Top 104;wind_speed [m/s]",i)
    AgregarLista(list_of_index,"Anemo 100;wind_speed [m/s]",i)
    AgregarLista(list_of_index,"Anemo 90;wind_speed [m/s]",i)
    AgregarLista(list_of_index,"Anemo 60;wind_speed [m/s]",i)
    AgregarLista(list_of_index,"Anemo 30;wind_speed [m/s]",i)
    AgregarLista(list_of_index,"Veleta 80;wind_direction [A]",i)
    AgregarLista(list_of_index,"Veleta 40;wind_direction [A]",i)
    AgregarLista(list_of_index,"Hygro/Thermo;temperature [A]",i)
    AgregarLista(list_of_index,"Barometro;air_pressure [mbar]",i)
    AgregarLista(list_of_index,"Humidity;voltage [% rel]",i) ##
    AgregarLista(list_of_index,"A1",i)
    AgregarLista(list_of_index,"A2",i)
    AgregarLista(list_of_index,"A3",i)
    AgregarLista(list_of_index,"C1",i)
    AgregarLista(list_of_index,"C2",i)
    AgregarLista(list_of_index,"C3",i)
    AgregarLista(list_of_index,"C4",i)
    AgregarLista(list_of_index,"C5",i)
    AgregarLista(list_of_index,"D1",i)
    AgregarLista(list_of_index,"D2",i)
    AgregarLista(list_of_index,"V",i)

df.drop(df.index[list_of_index],inplace=True)
#casteando a float64 variables de tipo object
df["Veleta 80;wind_direction [A]"]=pd.to_numeric(df["Veleta 80;wind_direction [A]"],errors='coerce').fillna(0)
df["Veleta 40;wind_direction [A]"]=pd.to_numeric(df["Veleta 40;wind_direction [A]"],errors='coerce').fillna(0)
df["Hygro/Thermo;temperature [A]"]=pd.to_numeric(df["Hygro/Thermo;temperature [A]"],errors='coerce').fillna(0)
df["Barometro;air_pressure [mbar]"]=pd.to_numeric(df["Barometro;air_pressure [mbar]"],errors='coerce').fillna(0)
df["Humidity;voltage [% rel]"]=pd.to_numeric(df["Humidity;voltage [% rel]"],errors='coerce').fillna(0)


for i in df.index:
    #SUMA POR COLUMNA 
    sumAT104 = sumAT104 + float(df.loc[i,"Anemo Top 104;wind_speed [m/s]"])
    sumAT100 = sumAT100 + float(df.loc[i,"Anemo 100;wind_speed [m/s]"])
    sumAT90 = sumAT90 + float(df.loc[i,"Anemo 90;wind_speed [m/s]"])
    sumAT60 = sumAT60 + float(df.loc[i,"Anemo 60;wind_speed [m/s]"])
    sumAT30 = sumAT30 + float(df.loc[i,"Anemo 30;wind_speed [m/s]"])
    sumVel80 = sumVel80 + (df.loc[i,"Veleta 80;wind_direction [A]"])
    sumVel40 = sumVel40 + (df.loc[i,"Veleta 40;wind_direction [A]"])
    sumHygro = sumHygro + (df.loc[i,"Hygro/Thermo;temperature [A]"])
    sumBar = sumBar +  (df.loc[i,"Barometro;air_pressure [mbar]"])
    sumHum = sumHum + (df.loc[i,"Humidity;voltage [% rel]"])

#cantidad de filas del csv
CantElementos= len(df.index)

def ejecutarScript():
    #MEDIAS
    MediaAT104 = sumAT104/CantElementos
    MediaAT100 = sumAT100/CantElementos
    MediaAT90 = sumAT90/CantElementos
    MediaAt30 = sumAT30/CantElementos
    MediaVel80 = sumVel80/CantElementos
    MediaVel40 = sumVel40/CantElementos
    MediaHygro = sumHygro/CantElementos
    MediaBar = sumBar/CantElementos
    MediaHum = sumHum/CantElementos

    #DESVIACION ESTANDAR
    DesvAT104 = df["Anemo Top 104;wind_speed [m/s]"].std()
    DesvAT100 = df["Anemo 100;wind_speed [m/s]"].std()
    DesvAT90 = df["Anemo 90;wind_speed [m/s]"].std()
    DesvAT60 = df["Anemo 60;wind_speed [m/s]"].std()
    DesvAT30 = df["Anemo 30;wind_speed [m/s]"].std()
    DesvVel80 = df["Veleta 80;wind_direction [A]"].std()
    DesvVel40 = df["Veleta 40;wind_direction [A]"].std()
    DesvHygro = df["Hygro/Thermo;temperature [A]"].std()
    DesvBar = df["Barometro;air_pressure [mbar]"].std()
    DesvHum = df["Humidity;voltage [% rel]"].std()

    #MAXIMOS 
    MaxAT104 = df["Anemo Top 104;wind_speed [m/s]"].max()
    MaxAT100 = df["Anemo 100;wind_speed [m/s]"].max()
    MaxAT90 = df["Anemo 90;wind_speed [m/s]"].max()
    MaxAT60 = df["Anemo 60;wind_speed [m/s]"].max()
    MaxAT30 = df["Anemo 30;wind_speed [m/s]"].max()
    MaxVel80 = df["Veleta 80;wind_direction [A]"].max()
    MaxVel40 = df["Veleta 40;wind_direction [A]"].max()
    MaxHygro = df["Hygro/Thermo;temperature [A]"].max()
    MaxBar = df["Barometro;air_pressure [mbar]"].max()
    MaxHum = df["Humidity;voltage [% rel]"].max()

    #MINIMOS
    MinAT104 = df["Anemo Top 104;wind_speed [m/s]"].min()
    MinAT100 = df["Anemo 100;wind_speed [m/s]"].min()
    MinAT90 = df["Anemo 90;wind_speed [m/s]"].min()
    MinAT60 = df["Anemo 60;wind_speed [m/s]"].min()
    MinAT30 = df["Anemo 30;wind_speed [m/s]"].min()
    MinVel80 = df["Veleta 80;wind_direction [A]"].min()
    MinVel40 = df["Veleta 40;wind_direction [A]"].min()
    MinHygro = df["Hygro/Thermo;temperature [A]"].min()
    MinBar = df["Barometro;air_pressure [mbar]"].min()
    MinHum = df["Humidity;voltage [% rel]"].min()
    #print(MediaAT104," ",MediaAT100," ",MediaAT90," ",MediaAt30," ",MediaVel80," ",MediaVel40)
    
    print("Actualizado")
    time.sleep(600) #Se actualiza cada 10 minutos 

while True:
    ejecutarScript()
#df.to_csv(r'MedidasPLC2.csv',sep =";",index=False, header=True)
print("listo")