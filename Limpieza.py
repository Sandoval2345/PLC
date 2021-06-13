from enum import auto
from numpy import append, float64, int64, intp, left_shift, source
import pandas as pd
from pandas.io.formats.format import EastAsianTextAdjustment
import sqlalchemy
import psycopg2
import sqlite3
import time
import itertools
import numpy
from psycopg2.extensions import register_adapter, AsIs
import statistics

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

#cada elemento de list es la suma de los valores de cada columna cada 600seg (10min), listOf contiene los valores de los 10 min
sumAT104 = 0
listAT104 = []
listOfAT104 = []
listDesvAT104 = []
listMaxAT104 =[]
listMinAT104 = []
listPromAT104 = []
sumAT100 = 0
listAT100 = []
listOfAT100 = []
listDesvAT100 = []
listMaxAT100 =[]
listMinAT100 = []
sumAT90 = 0
listAT90 = []
listOfAT90 = []
listDesvAT90 = []
listMaxAT90 =[]
listMinAT90 = []
sumAT60 = 0
listAT60 = []
listOfAT60 = []
listDesvAT60 = []
listMaxAT60 =[]
listMinAT60 = []
sumAT30 = 0
listAT30 = []
listOfAT30 = []
listDesvAT30 = []
listMaxAT30 =[]
listMinAT30 = []
sumVel80 = 0
listVel80 = []
listOfVel80 = []
listDesvVel80 = []
listMaxVel80 =[]
listMinVel80 = []
sumVel40 = 0
listVel40 = []
listOfVel40 = []
listDesvVel40 = []
listMaxVel40 =[]
listMinVel40 = []
sumHygro = 0
listHygro = []
listOfHygro = []
listDesvHygro = []
listMaxHygro =[]
listMinHygro = []
sumBar = 0
listBar = []
listOfBar = []
listDesvBar = []
listMaxBar =[]
listMinBar = []
sumHum = 0
listHum = []
listOfHum = []
listDesvHum = []
listMaxHum =[]
listMinHum= []
count = 1
print((df.loc[0,"Anemo Top 104;wind_speed [m/s]"]))
print((df.loc[600,"Anemo Top 104;wind_speed [m/s]"]))
for i in df.index:
    #SUMA POR COLUMNA 
    sumAT104 = sumAT104 + float(df.loc[i,"Anemo Top 104;wind_speed [m/s]"])
    listOfAT104.append(float(df.loc[i,"Anemo Top 104;wind_speed [m/s]"]))
    sumAT100 = sumAT100 + float(df.loc[i,"Anemo 100;wind_speed [m/s]"])
    listOfAT100.append(float(df.loc[i,"Anemo 100;wind_speed [m/s]"]))
    sumAT90 = sumAT90 + float(df.loc[i,"Anemo 90;wind_speed [m/s]"])
    listOfAT90.append(float(df.loc[i,"Anemo 90;wind_speed [m/s]"]))
    sumAT60 = sumAT60 + float(df.loc[i,"Anemo 60;wind_speed [m/s]"])
    listOfAT60.append(float(df.loc[i,"Anemo 60;wind_speed [m/s]"]))
    sumAT30 = sumAT30 + float(df.loc[i,"Anemo 30;wind_speed [m/s]"])
    listOfAT30.append(float(df.loc[i,"Anemo 30;wind_speed [m/s]"]))
    sumVel80 = sumVel80 + (df.loc[i,"Veleta 80;wind_direction [A]"])
    listOfVel80.append((df.loc[i,"Veleta 80;wind_direction [A]"]))
    sumVel40 = sumVel40 + (df.loc[i,"Veleta 40;wind_direction [A]"])
    listOfVel40.append((df.loc[i,"Veleta 40;wind_direction [A]"]))
    sumHygro = sumHygro + (df.loc[i,"Hygro/Thermo;temperature [A]"])
    listOfHygro.append((df.loc[i,"Hygro/Thermo;temperature [A]"]))
    sumBar = sumBar +  (df.loc[i,"Barometro;air_pressure [mbar]"])
    listOfBar.append((df.loc[i,"Barometro;air_pressure [mbar]"]))
    sumHum = sumHum + (df.loc[i,"Humidity;voltage [% rel]"])
    listOfHum.append((df.loc[i,"Humidity;voltage [% rel]"]))
    
    if (count % 600 == 0): #obteniendo las sumas de cada columna cada 600 seg (10 min)
        #print(count)
        #print(listOfAT104[0],",", listOfAT104[600])
        listAT104.append(sumAT104)
        listDesvAT104.append(statistics.stdev(listOfAT104)) 
        listMaxAT104.append(max(listOfAT104))
        listMinAT104.append(min(listOfAT104))
        listPromAT104.append(statistics.mean(listOfAT104))
        listOfAT104.clear()

        listAT100.append(sumAT100)
        listDesvAT100.append(statistics.stdev(listOfAT100))
        listMaxAT100.append(max(listOfAT100))
        listMinAT100.append(min(listOfAT100))
        listOfAT100.clear()

        listAT90.append(sumAT90)
        listDesvAT90.append(statistics.stdev(listOfAT90))
        listMaxAT90.append(max(listOfAT90))
        listMinAT90.append(min(listOfAT90))
        listOfAT90.clear()

        listAT60.append(sumAT60)
        listDesvAT60.append(statistics.stdev(listOfAT60))
        listMaxAT60.append(max(listOfAT60))
        listMinAT60.append(min(listOfAT60))
        listOfAT60.clear()

        listAT30.append(sumAT30)
        listDesvAT30.append(statistics.stdev(listOfAT30))
        listMaxAT30.append(max(listOfAT30))
        listMinAT30.append(min(listOfAT30))
        listOfAT30.clear()

        listVel80.append(sumVel80)
        listDesvVel80.append(statistics.stdev(listOfVel80))
        listMaxVel80.append(max(listOfVel80))
        listMinVel80.append(min(listOfVel80))
        listOfVel80.clear()

        listVel40.append(sumVel40)
        listDesvVel40.append(statistics.stdev(listOfVel40))
        listMaxVel40.append(max(listOfVel40))
        listMinVel40.append(min(listOfVel40))
        listOfVel40.clear()

        listHygro.append(sumHygro)
        listDesvHygro.append(statistics.stdev(listOfHygro))
        listMaxHygro.append(max(listOfHygro))
        listMinHygro.append(min(listOfHygro))
        listOfHygro.clear()

        listBar.append(sumBar)
        listDesvBar.append(statistics.stdev(listOfBar))
        listMaxBar.append(max(listOfBar))
        listMinBar.append(min(listOfBar))
        listOfHygro.clear()

        listHum.append(sumHum)
        listDesvHum.append(statistics.stdev(listOfHum))
        listMaxHum.append(max(listOfHum))
        listMinHum.append(min(listOfHum))
        listOfHum.clear()
    count +=1


#cantidad de filas del csv
CantElementos= len(df.index)


#df.to_csv(r'MedidasPLC2.csv',sep =";",index=False, header=True)
print("listo")