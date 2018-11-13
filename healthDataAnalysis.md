

```python
sc.stop()
```


```python
import os
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
```


```python
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Health").setMaster("local[4]")  #local or hive
sc = SparkContext.getOrCreate(conf=conf) # if already running then get else get
```


```python
from pyspark.sql import SparkSession   # if only SparkSession is needed
spark = SparkSession.builder.appName('Health').getOrCreate()
```


```python
df = spark.read.csv("file:///home/piyush/Downloads/health.csv", header = True)
```


```python
df.describe()  #df.collect(), df.take(2)
```




    DataFrame[summary: string, S.No.: string, State/ UT: string, Total Population in Rural Areas: string, Estimated Tribal Population in Rural Areas: string, Sub Centres - Required: string, Sub Centres - In Position: string, Sub Centres - Shortfall: string, Primary Health Centres (PHCs) - Required: string, Primary Health Centres (PHCs) - In Position: string, Primary Health Centres (PHCs) - Shortfall: string, Community Health Centres (CHCs) - Required: string, Community Health Centres (CHCs) - In Position: string, Community Health Centres (CHCs) - Shortfall: string]



**Renaming Columns 


```python
# Renaming column names a/c to my wishes!
oldColumns = df.schema.names
newColumns = ["sn", "state", "population", "tribal_population", "sub_centres_req","sub_centres_present", \
              "sub_centres_short", "phc_req", "phc_present", "phc_short", "chc_req", "chc_present" \
             ,"chc_short"]


```


```python
# Part of Renaming collumns
from functools import reduce
df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)),df)
df.printSchema()
df.show()
```

    root
     |-- sn: string (nullable = true)
     |-- state: string (nullable = true)
     |-- population: string (nullable = true)
     |-- tribal_population: string (nullable = true)
     |-- sub_centres_req: string (nullable = true)
     |-- sub_centres_present: string (nullable = true)
     |-- sub_centres_short: string (nullable = true)
     |-- phc_req: string (nullable = true)
     |-- phc_present: string (nullable = true)
     |-- phc_short: string (nullable = true)
     |-- chc_req: string (nullable = true)
     |-- chc_present: string (nullable = true)
     |-- chc_short: string (nullable = true)
    
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    | sn|            state|population|tribal_population|sub_centres_req|sub_centres_present|sub_centres_short|phc_req|phc_present|phc_short|chc_req|chc_present|chc_short|
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    |  1|   Andhra Pradesh|56,311,788|        4,723,312|         11,892|             12,522|          Surplus|  1,955|      1,624|      331|    488|        281|      207|
    |  2|Arunachal Pradesh| 1,069,165|          744,996|            313|                286|               27|     48|         97|  Surplus|     12|         48|  Surplus|
    |  3|            Assam|26,780,516|        3,638,841|          5,841|              4,604|            1,237|    953|        938|       15|    238|        108|      130|
    |  4|            Bihar|92,075,028|          889,200|         18,533|              9,696|            8,837|  3,083|      1,863|    1,220|    770|         70|      700|
    |  5|     Chhattisgarh|19,603,658|        7,377,058|          4,904|              5,076|          Surplus|    776|        741|       35|    194|        148|       46|
    |  6|              Goa|   551,414|              155|            110|                175|          Surplus|     18|         19|  Surplus|      4|          5|  Surplus|
    |  7|          Gujarat|34,670,817|        7,500,509|          7,934|              7,274|              660|  1,280|      1,123|      157|    320|        305|       15|
    |  8|          Haryana|16,531,493|                0|          3,306|              2,508|              798|    551|        444|      107|    137|        107|       30|
    |  9| Himachal Pradesh| 6,167,805|          266,701|          1,269|              2,067|          Surplus|    210|        453|  Surplus|     52|         76|  Surplus|
    | 10|  Jammu & Kashmir| 9,134,820|        1,262,945|          1,995|              1,907|               88|    325|        397|  Surplus|     81|         83|  Surplus|
    | 11|        Jharkhand|25,036,946|        7,767,269|          6,043|              3,958|            2,085|    964|        330|      634|    241|        188|       53|
    | 12|        Karnataka|37,552,529|        3,158,558|          7,931|              8,870|          Surplus|  1,304|      2,310|  Surplus|    326|        180|      146|
    | 13|           Kerala|17,455,506|          259,169|          3,525|              4,575|          Surplus|    586|        809|  Surplus|    146|        224|  Surplus|
    | 14|   Madhya Pradesh|52,537,899|       13,550,258|         12,314|              8,869|            3,445|  1,977|      1,156|      821|    494|        333|      161|
    | 15|      Maharashtra|61,545,441|        8,260,697|         13,410|             10,580|            2,830|  2,189|      1,809|      380|    547|        365|      182|
    | 16|          Manipur| 1,899,624|          842,941|            492|                420|               72|     77|         80|  Surplus|     19|         16|        3|
    | 17|        Meghalaya| 2,368,971|        2,137,702|            758|                405|              353|    114|        109|        5|     28|         29|  Surplus|
    | 18|          Mizoram|   529,037|          509,316|            173|                370|          Surplus|     26|         57|  Surplus|      6|          9|  Surplus|
    | 19|         Nagaland| 1,406,861|        1,318,698|            457|                396|               61|     68|        126|  Surplus|     17|         21|  Surplus|
    | 20|           Odisha|34,951,234|        8,599,849|          8,136|              6,688|            1,448|  1,308|      1,228|       80|    327|        377|  Surplus|
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    only showing top 20 rows
    


**To use regex replace


```python
# To use regex replace
import re
from pyspark.sql.functions import *
```

# df1 = df after (1) removing ',' (2) Converting Surplus to 0  (3) Typing casting 


```python
df1 = df
from pyspark.sql.types import *
colnames = df1.schema.names
for i in colnames:
    colname = i
    df1 = df1.withColumn(colname, regexp_replace(colname, ",", ""))  #Removing spaces from data in columns
    df1 = df1.withColumn(colname, regexp_replace(colname, "Surplus", "0")) # Modyfying unwanted data
    if(colname!='state'):
        df1 = df1.withColumn(colname, col(colname).cast(IntegerType())) #Type casting 
```


```python
df.show()  # Orignal data frame
df1.show() # Data frame after : 1) Type casting 2) filtering Data
df1.printSchema()
```

    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    | sn|            state|population|tribal_population|sub_centres_req|sub_centres_present|sub_centres_short|phc_req|phc_present|phc_short|chc_req|chc_present|chc_short|
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    |  1|   Andhra Pradesh|56,311,788|        4,723,312|         11,892|             12,522|          Surplus|  1,955|      1,624|      331|    488|        281|      207|
    |  2|Arunachal Pradesh| 1,069,165|          744,996|            313|                286|               27|     48|         97|  Surplus|     12|         48|  Surplus|
    |  3|            Assam|26,780,516|        3,638,841|          5,841|              4,604|            1,237|    953|        938|       15|    238|        108|      130|
    |  4|            Bihar|92,075,028|          889,200|         18,533|              9,696|            8,837|  3,083|      1,863|    1,220|    770|         70|      700|
    |  5|     Chhattisgarh|19,603,658|        7,377,058|          4,904|              5,076|          Surplus|    776|        741|       35|    194|        148|       46|
    |  6|              Goa|   551,414|              155|            110|                175|          Surplus|     18|         19|  Surplus|      4|          5|  Surplus|
    |  7|          Gujarat|34,670,817|        7,500,509|          7,934|              7,274|              660|  1,280|      1,123|      157|    320|        305|       15|
    |  8|          Haryana|16,531,493|                0|          3,306|              2,508|              798|    551|        444|      107|    137|        107|       30|
    |  9| Himachal Pradesh| 6,167,805|          266,701|          1,269|              2,067|          Surplus|    210|        453|  Surplus|     52|         76|  Surplus|
    | 10|  Jammu & Kashmir| 9,134,820|        1,262,945|          1,995|              1,907|               88|    325|        397|  Surplus|     81|         83|  Surplus|
    | 11|        Jharkhand|25,036,946|        7,767,269|          6,043|              3,958|            2,085|    964|        330|      634|    241|        188|       53|
    | 12|        Karnataka|37,552,529|        3,158,558|          7,931|              8,870|          Surplus|  1,304|      2,310|  Surplus|    326|        180|      146|
    | 13|           Kerala|17,455,506|          259,169|          3,525|              4,575|          Surplus|    586|        809|  Surplus|    146|        224|  Surplus|
    | 14|   Madhya Pradesh|52,537,899|       13,550,258|         12,314|              8,869|            3,445|  1,977|      1,156|      821|    494|        333|      161|
    | 15|      Maharashtra|61,545,441|        8,260,697|         13,410|             10,580|            2,830|  2,189|      1,809|      380|    547|        365|      182|
    | 16|          Manipur| 1,899,624|          842,941|            492|                420|               72|     77|         80|  Surplus|     19|         16|        3|
    | 17|        Meghalaya| 2,368,971|        2,137,702|            758|                405|              353|    114|        109|        5|     28|         29|  Surplus|
    | 18|          Mizoram|   529,037|          509,316|            173|                370|          Surplus|     26|         57|  Surplus|      6|          9|  Surplus|
    | 19|         Nagaland| 1,406,861|        1,318,698|            457|                396|               61|     68|        126|  Surplus|     17|         21|  Surplus|
    | 20|           Odisha|34,951,234|        8,599,849|          8,136|              6,688|            1,448|  1,308|      1,228|       80|    327|        377|  Surplus|
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    only showing top 20 rows
    
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    | sn|            state|population|tribal_population|sub_centres_req|sub_centres_present|sub_centres_short|phc_req|phc_present|phc_short|chc_req|chc_present|chc_short|
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    |  1|   Andhra Pradesh|  56311788|          4723312|          11892|              12522|                0|   1955|       1624|      331|    488|        281|      207|
    |  2|Arunachal Pradesh|   1069165|           744996|            313|                286|               27|     48|         97|        0|     12|         48|        0|
    |  3|            Assam|  26780516|          3638841|           5841|               4604|             1237|    953|        938|       15|    238|        108|      130|
    |  4|            Bihar|  92075028|           889200|          18533|               9696|             8837|   3083|       1863|     1220|    770|         70|      700|
    |  5|     Chhattisgarh|  19603658|          7377058|           4904|               5076|                0|    776|        741|       35|    194|        148|       46|
    |  6|              Goa|    551414|              155|            110|                175|                0|     18|         19|        0|      4|          5|        0|
    |  7|          Gujarat|  34670817|          7500509|           7934|               7274|              660|   1280|       1123|      157|    320|        305|       15|
    |  8|          Haryana|  16531493|                0|           3306|               2508|              798|    551|        444|      107|    137|        107|       30|
    |  9| Himachal Pradesh|   6167805|           266701|           1269|               2067|                0|    210|        453|        0|     52|         76|        0|
    | 10|  Jammu & Kashmir|   9134820|          1262945|           1995|               1907|               88|    325|        397|        0|     81|         83|        0|
    | 11|        Jharkhand|  25036946|          7767269|           6043|               3958|             2085|    964|        330|      634|    241|        188|       53|
    | 12|        Karnataka|  37552529|          3158558|           7931|               8870|                0|   1304|       2310|        0|    326|        180|      146|
    | 13|           Kerala|  17455506|           259169|           3525|               4575|                0|    586|        809|        0|    146|        224|        0|
    | 14|   Madhya Pradesh|  52537899|         13550258|          12314|               8869|             3445|   1977|       1156|      821|    494|        333|      161|
    | 15|      Maharashtra|  61545441|          8260697|          13410|              10580|             2830|   2189|       1809|      380|    547|        365|      182|
    | 16|          Manipur|   1899624|           842941|            492|                420|               72|     77|         80|        0|     19|         16|        3|
    | 17|        Meghalaya|   2368971|          2137702|            758|                405|              353|    114|        109|        5|     28|         29|        0|
    | 18|          Mizoram|    529037|           509316|            173|                370|                0|     26|         57|        0|      6|          9|        0|
    | 19|         Nagaland|   1406861|          1318698|            457|                396|               61|     68|        126|        0|     17|         21|        0|
    | 20|           Odisha|  34951234|          8599849|           8136|               6688|             1448|   1308|       1228|       80|    327|        377|        0|
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    only showing top 20 rows
    
    root
     |-- sn: integer (nullable = true)
     |-- state: string (nullable = true)
     |-- population: integer (nullable = true)
     |-- tribal_population: integer (nullable = true)
     |-- sub_centres_req: integer (nullable = true)
     |-- sub_centres_present: integer (nullable = true)
     |-- sub_centres_short: integer (nullable = true)
     |-- phc_req: integer (nullable = true)
     |-- phc_present: integer (nullable = true)
     |-- phc_short: integer (nullable = true)
     |-- chc_req: integer (nullable = true)
     |-- chc_present: integer (nullable = true)
     |-- chc_short: integer (nullable = true)
    


**Creating Table from dataframe 


```python
df1.registerTempTable("df1_table")
```

### Query 1


```python
#List of states that have  shortage of sub_centres , phc_centres and chc centres 
check = spark.sql("select state,sub_centres_short,phc_short,chc_short from df1_table where sub_centres_short>0 and phc_short>0 and chc_short>0 ")
(check).show()
```

    +--------------------+-----------------+---------+---------+
    |               state|sub_centres_short|phc_short|chc_short|
    +--------------------+-----------------+---------+---------+
    |               Assam|             1237|       15|      130|
    |               Bihar|             8837|     1220|      700|
    |             Gujarat|              660|      157|       15|
    |             Haryana|              798|      107|       30|
    |           Jharkhand|             2085|      634|       53|
    |      Madhya Pradesh|             3445|      821|      161|
    |         Maharashtra|             2830|      380|      182|
    |              Punjab|              513|      131|       15|
    |             Tripura|               41|       27|       15|
    |       Uttar Pradesh|            10516|     1480|      778|
    |         West Bengal|             2680|     1239|      189|
    |Dadra & Nagar Haveli|                4|        2|        1|
    |               Delhi|               42|        5|        3|
    +--------------------+-----------------+---------+---------+
    


### Query 2,3,4,5


```python
# States with no shortage of sub_centres,phc,chc
check2 = spark.sql("select state from df1_table where sub_centres_short==0 and phc_short==0 and chc_short==0 ")
#Top 5 most populated states
check3 = spark.sql("select state,population from df1_table order by population desc limit 5")
#State with maximum tribal population
check4 = spark.sql("select state,tribal_population from df1_table order by tribal_population desc limit 1")

#all details of a Lakshadweep
check5 = spark.sql("select *  from df1_table where state='Lakshadweep'")
check5.show()

```

    +---+-----------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    | sn|      state|population|tribal_population|sub_centres_req|sub_centres_present|sub_centres_short|phc_req|phc_present|phc_short|chc_req|chc_present|chc_short|
    +---+-----------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    | 34|Lakshadweep|     14121|            13503|              4|                 14|                0|      0|          4|        0|      0|          3|        0|
    +---+-----------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    


**Filtering Data


```python
check = check['sub_centres_short','phc_short','chc_short'] #filtering data
check.show()
```

    +-----------------+---------+---------+
    |sub_centres_short|phc_short|chc_short|
    +-----------------+---------+---------+
    |             1237|       15|      130|
    |             8837|     1220|      700|
    |              660|      157|       15|
    |              798|      107|       30|
    |             2085|      634|       53|
    |             3445|      821|      161|
    |             2830|      380|      182|
    |              513|      131|       15|
    |               41|       27|       15|
    |            10516|     1480|      778|
    |             2680|     1239|      189|
    |                4|        2|        1|
    |               42|        5|        3|
    +-----------------+---------+---------+
    


**Plotting Graph


```python
# Plotting Graph 
import pandas
import matplotlib.pyplot as plt
```


```python
import numpy as np
```

**Increasing size of graph


```python
# Get current size
fig_size = plt.rcParams["figure.figsize"]
print("Current_size:", fig_size)
#Set figure width to 10 and height to 6
fig_size[0] = 10
fig_size[1] = 9
plt.rcParams["figure.figsize"]=  fig_size
```

    Current_size: [10.0, 9.0]


**Removing states with small values of (sub_centres_short) (To make graph look better)


```python
df1_mod = df1.filter("sub_centres_short>=798")
df1_mod.show()
```

    +---+--------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    | sn|         state|population|tribal_population|sub_centres_req|sub_centres_present|sub_centres_short|phc_req|phc_present|phc_short|chc_req|chc_present|chc_short|
    +---+--------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    |  3|         Assam|  26780516|          3638841|           5841|               4604|             1237|    953|        938|       15|    238|        108|      130|
    |  4|         Bihar|  92075028|           889200|          18533|               9696|             8837|   3083|       1863|     1220|    770|         70|      700|
    |  8|       Haryana|  16531493|                0|           3306|               2508|              798|    551|        444|      107|    137|        107|       30|
    | 11|     Jharkhand|  25036946|          7767269|           6043|               3958|             2085|    964|        330|      634|    241|        188|       53|
    | 14|Madhya Pradesh|  52537899|         13550258|          12314|               8869|             3445|   1977|       1156|      821|    494|        333|      161|
    | 15|   Maharashtra|  61545441|          8260697|          13410|              10580|             2830|   2189|       1809|      380|    547|        365|      182|
    | 20|        Odisha|  34951234|          8599849|           8136|               6688|             1448|   1308|       1228|       80|    327|        377|        0|
    | 27| Uttar Pradesh| 155111022|           112898|          31037|              20521|            10516|   5172|       3692|     1480|   1293|        515|      778|
    | 28|   West Bengal|  62213676|          4456160|          13036|              10356|             2680|   2148|        909|     1239|    537|        348|      189|
    +---+--------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    


**Converting Dataframe (df1) to Pandas for plotting graph


```python
df1_pd = df1_mod.toPandas()
# df2_pd["state"]
label = [i for i in df1_pd["state"]]
label
#OR
# label=[]
# for i in df2["state"]:
#     label.append(i)
```




    ['Assam',
     'Bihar',
     'Haryana',
     'Jharkhand',
     'Madhya Pradesh',
     'Maharashtra',
     'Odisha',
     'Uttar Pradesh',
     'West Bengal']



**Plotting Bar graph of : df1 vs state

## numpy.arange(index)
**1)returns Array of evenly spaced values from 0 to stop or from start to stop
2)used here to give x-cordinates to graph i.e 0, 1, 2 till all elements in the list
3)So in following line of plt.bar(index,df1_pd["sub_centres_short"] )
4)first we give stream of abcissa, then set of ordinate.So we have passed an array of int first then we pass an array of values of "sub_centres_short 
5)We have stored array of states in 'label' variable  
6)plt.xlabel() : simply for labely x-axis
7)plt.ylabel(): for labeling y-axis
9)xticks(): For giving specs of bars of bar-graph
            index: array storing all s-co-ordinates
            label: array of states
            


```python
index=np.arange(len(label)) 
plt.bar(index, df1_pd["sub_centres_short"])  # Or change to df2
plt.xlabel("States",size=15)
plt.ylabel("Sub Centres - Shortfall", size=15)
plt.xticks(index, label, fontsize=15, rotation=30)
plt.yticks(fontsize = 15)
plt.show()

```


![png](output_33_0.png)


# First analysis of sub_centre values
# df2 = df with modified '_short' column i.e contains -ve , +ve values 
## It contains both surplus, and shortage values


```python
df2 = df1
df2 = df2.withColumn('sub_centres_short', col('sub_centres_present')-col('sub_centres_req'))
df2 = df2.withColumn('phc_short', col('phc_present')-col('phc_req'))
df2 = df2.withColumn('chc_short', col('chc_present')-col('chc_req'))
df2.show()
df2.printSchema()
```

    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    | sn|            state|population|tribal_population|sub_centres_req|sub_centres_present|sub_centres_short|phc_req|phc_present|phc_short|chc_req|chc_present|chc_short|
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    |  1|   Andhra Pradesh|  56311788|          4723312|          11892|              12522|              630|   1955|       1624|     -331|    488|        281|     -207|
    |  2|Arunachal Pradesh|   1069165|           744996|            313|                286|              -27|     48|         97|       49|     12|         48|       36|
    |  3|            Assam|  26780516|          3638841|           5841|               4604|            -1237|    953|        938|      -15|    238|        108|     -130|
    |  4|            Bihar|  92075028|           889200|          18533|               9696|            -8837|   3083|       1863|    -1220|    770|         70|     -700|
    |  5|     Chhattisgarh|  19603658|          7377058|           4904|               5076|              172|    776|        741|      -35|    194|        148|      -46|
    |  6|              Goa|    551414|              155|            110|                175|               65|     18|         19|        1|      4|          5|        1|
    |  7|          Gujarat|  34670817|          7500509|           7934|               7274|             -660|   1280|       1123|     -157|    320|        305|      -15|
    |  8|          Haryana|  16531493|                0|           3306|               2508|             -798|    551|        444|     -107|    137|        107|      -30|
    |  9| Himachal Pradesh|   6167805|           266701|           1269|               2067|              798|    210|        453|      243|     52|         76|       24|
    | 10|  Jammu & Kashmir|   9134820|          1262945|           1995|               1907|              -88|    325|        397|       72|     81|         83|        2|
    | 11|        Jharkhand|  25036946|          7767269|           6043|               3958|            -2085|    964|        330|     -634|    241|        188|      -53|
    | 12|        Karnataka|  37552529|          3158558|           7931|               8870|              939|   1304|       2310|     1006|    326|        180|     -146|
    | 13|           Kerala|  17455506|           259169|           3525|               4575|             1050|    586|        809|      223|    146|        224|       78|
    | 14|   Madhya Pradesh|  52537899|         13550258|          12314|               8869|            -3445|   1977|       1156|     -821|    494|        333|     -161|
    | 15|      Maharashtra|  61545441|          8260697|          13410|              10580|            -2830|   2189|       1809|     -380|    547|        365|     -182|
    | 16|          Manipur|   1899624|           842941|            492|                420|              -72|     77|         80|        3|     19|         16|       -3|
    | 17|        Meghalaya|   2368971|          2137702|            758|                405|             -353|    114|        109|       -5|     28|         29|        1|
    | 18|          Mizoram|    529037|           509316|            173|                370|              197|     26|         57|       31|      6|          9|        3|
    | 19|         Nagaland|   1406861|          1318698|            457|                396|              -61|     68|        126|       58|     17|         21|        4|
    | 20|           Odisha|  34951234|          8599849|           8136|               6688|            -1448|   1308|       1228|      -80|    327|        377|       50|
    +---+-----------------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
    only showing top 20 rows
    
    root
     |-- sn: integer (nullable = true)
     |-- state: string (nullable = true)
     |-- population: integer (nullable = true)
     |-- tribal_population: integer (nullable = true)
     |-- sub_centres_req: integer (nullable = true)
     |-- sub_centres_present: integer (nullable = true)
     |-- sub_centres_short: integer (nullable = true)
     |-- phc_req: integer (nullable = true)
     |-- phc_present: integer (nullable = true)
     |-- phc_short: integer (nullable = true)
     |-- chc_req: integer (nullable = true)
     |-- chc_present: integer (nullable = true)
     |-- chc_short: integer (nullable = true)
    


**breaking df2 into two data frames: (1)surplus (2)shortage Also removing negative sign from shortage


```python
df2_surplus = df2.filter("sub_centres_short>=0")
df2_surplus.printSchema()
```

    root
     |-- sn: integer (nullable = true)
     |-- state: string (nullable = true)
     |-- population: integer (nullable = true)
     |-- tribal_population: integer (nullable = true)
     |-- sub_centres_req: integer (nullable = true)
     |-- sub_centres_present: integer (nullable = true)
     |-- sub_centres_short: integer (nullable = true)
     |-- phc_req: integer (nullable = true)
     |-- phc_present: integer (nullable = true)
     |-- phc_short: integer (nullable = true)
     |-- chc_req: integer (nullable = true)
     |-- chc_present: integer (nullable = true)
     |-- chc_short: integer (nullable = true)
    


**The plot showed a problem that the Y axis is not ordered. :
    SOLUTION: Since data type was strings , so type cast it to int


```python
 #Now removing negative sign also
    
df2_shortage= df2.filter("sub_centres_short<0") #Initialising 

from pyspark.sql.types import *
colnames = df2_shortage.schema.names
for i in colnames:
    colname = i
    if(colname=='sub_centres_short'):
        df2_shortage = df2_shortage.withColumn(colname, regexp_replace(colname, "-", ""))  #Removing spaces from data in columns
        df2_shortage = df2_shortage.withColumn(colname, col(colname).cast(IntegerType())) #Type casting
df2_shortage.printSchema()
```

    root
     |-- sn: integer (nullable = true)
     |-- state: string (nullable = true)
     |-- population: integer (nullable = true)
     |-- tribal_population: integer (nullable = true)
     |-- sub_centres_req: integer (nullable = true)
     |-- sub_centres_present: integer (nullable = true)
     |-- sub_centres_short: integer (nullable = true)
     |-- phc_req: integer (nullable = true)
     |-- phc_present: integer (nullable = true)
     |-- phc_short: integer (nullable = true)
     |-- chc_req: integer (nullable = true)
     |-- chc_present: integer (nullable = true)
     |-- chc_short: integer (nullable = true)
    


**Converting Dataframe (df2) to Pandas for plotting graph


```python
df2_surplus_pd  = df2_surplus.toPandas()

# df2_shortage = df2_shortage["sub_centres_short"]
df2_shortage_pd = df2_shortage.toPandas()

label1 = [i for i in df2_surplus_pd["state"]]
label2 = [i for i in df2_shortage_pd["state"]]

# label1
# label2
```

**Ploting graph of df2_surplus


```python
index=np.arange(len(label1)) 
plt.bar(index, df2_surplus_pd["sub_centres_short"])  # Or change to df2
plt.xlabel("States",size=15)
plt.ylabel("Sub Centres - Surplus", size=15)
plt.xticks(index, label1, fontsize=15, rotation=90)
plt.yticks(fontsize = 15)
plt.show()

```


![png](output_43_0.png)


**Ploting graph of df2_shortage


```python
index=np.arange(len(label2)) 
plt.bar(index, df2_shortage_pd["sub_centres_short"])  # Or change to df2
plt.xlabel("States",size=15)
plt.ylabel("Sub Centres - Shortage", size=15)
plt.xticks(index, label2, fontsize=15, rotation=90)
plt.yticks(fontsize = 15)
plt.show()
```


![png](output_45_0.png)


# Now analysis of phc values
# df2 = df with modified '_short' column i.e contains -ve , +ve values 
## It contains both surplus, and shortage values

# REPEATING ABOVE STEPS

**breaking df2 into two data frames: (1)surplus (2)shortage Also removing negative sign from shortage


```python
df2_surplus = df2.filter("phc_short>=0")
df2_surplus.printSchema()
```

    root
     |-- sn: integer (nullable = true)
     |-- state: string (nullable = true)
     |-- population: integer (nullable = true)
     |-- tribal_population: integer (nullable = true)
     |-- sub_centres_req: integer (nullable = true)
     |-- sub_centres_present: integer (nullable = true)
     |-- sub_centres_short: integer (nullable = true)
     |-- phc_req: integer (nullable = true)
     |-- phc_present: integer (nullable = true)
     |-- phc_short: integer (nullable = true)
     |-- chc_req: integer (nullable = true)
     |-- chc_present: integer (nullable = true)
     |-- chc_short: integer (nullable = true)
    


**The plot showed a problem that the Y axis is not ordered. :
    SOLUTION: Since data type was strings , so type cast it to int


```python
 #Now removing negative sign also
    
df2_shortage= df2.filter("phc_short<0") #Initialising 

from pyspark.sql.types import *
colnames = df2_shortage.schema.names
for i in colnames:
    colname = i
    if(colname=='phc_short'):
        df2_shortage = df2_shortage.withColumn(colname, regexp_replace(colname, "-", ""))  #Removing spaces from data in columns
        df2_shortage = df2_shortage.withColumn(colname, col(colname).cast(IntegerType())) #Type casting
df2_shortage.printSchema()
```

    root
     |-- sn: integer (nullable = true)
     |-- state: string (nullable = true)
     |-- population: integer (nullable = true)
     |-- tribal_population: integer (nullable = true)
     |-- sub_centres_req: integer (nullable = true)
     |-- sub_centres_present: integer (nullable = true)
     |-- sub_centres_short: integer (nullable = true)
     |-- phc_req: integer (nullable = true)
     |-- phc_present: integer (nullable = true)
     |-- phc_short: integer (nullable = true)
     |-- chc_req: integer (nullable = true)
     |-- chc_present: integer (nullable = true)
     |-- chc_short: integer (nullable = true)
    


**Converting Dataframe (df2) to Pandas for plotting graph


```python
df2_surplus_pd  = df2_surplus.toPandas()

# df2_shortage = df2_shortage["sub_centres_short"]
df2_shortage_pd = df2_shortage.toPandas()

label1 = [i for i in df2_surplus_pd["state"]]
label2 = [i for i in df2_shortage_pd["state"]]

# label1
# label2
```

**Ploting graph of df2_surplus


```python
index=np.arange(len(label1)) 
plt.bar(index, df2_surplus_pd["phc_short"])  # Or change to df2
plt.xlabel("States",size=15)
plt.ylabel("PHC Centres - Surplus", size=15)
plt.xticks(index, label1, fontsize=15, rotation=90)
plt.yticks(fontsize = 15)
plt.show()

```


![png](output_54_0.png)


**Ploting graph of df2_shortage


```python
index=np.arange(len(label2)) 
plt.bar(index, df2_shortage_pd["phc_short"])  # Or change to df2
plt.xlabel("States",size=15)
plt.ylabel("PHC - Shortage", size=15)
plt.xticks(index, label2, fontsize=15, rotation=90)
plt.yticks(fontsize = 15)
plt.show()
```


![png](output_56_0.png)


# Now analysis of chc values
# df2 = df with modified '_short' column i.e contains -ve , +ve values 
## It contains both surplus, and shortage values

# REPEATING ABOVE STEPS

**breaking df2 into two data frames: (1)surplus (2)shortage Also removing negative sign from shortage


```python
df2_surplus = df2.filter("chc_short>=0")
df2_surplus.printSchema()
```

    root
     |-- sn: integer (nullable = true)
     |-- state: string (nullable = true)
     |-- population: integer (nullable = true)
     |-- tribal_population: integer (nullable = true)
     |-- sub_centres_req: integer (nullable = true)
     |-- sub_centres_present: integer (nullable = true)
     |-- sub_centres_short: integer (nullable = true)
     |-- phc_req: integer (nullable = true)
     |-- phc_present: integer (nullable = true)
     |-- phc_short: integer (nullable = true)
     |-- chc_req: integer (nullable = true)
     |-- chc_present: integer (nullable = true)
     |-- chc_short: integer (nullable = true)
    


**The plot showed a problem that the Y axis is not ordered. :
    SOLUTION: Since data type was strings , so type cast it to int


```python
 #Now removing negative sign also
    
df2_shortage= df2.filter("chc_short<0") #Initialising 

from pyspark.sql.types import *
colnames = df2_shortage.schema.names
for i in colnames:
    colname = i
    if(colname=='chc_short'):
        df2_shortage = df2_shortage.withColumn(colname, regexp_replace(colname, "-", ""))  #Removing spaces from data in columns
        df2_shortage = df2_shortage.withColumn(colname, col(colname).cast(IntegerType())) #Type casting
df2_shortage.printSchema()
```

    root
     |-- sn: integer (nullable = true)
     |-- state: string (nullable = true)
     |-- population: integer (nullable = true)
     |-- tribal_population: integer (nullable = true)
     |-- sub_centres_req: integer (nullable = true)
     |-- sub_centres_present: integer (nullable = true)
     |-- sub_centres_short: integer (nullable = true)
     |-- phc_req: integer (nullable = true)
     |-- phc_present: integer (nullable = true)
     |-- phc_short: integer (nullable = true)
     |-- chc_req: integer (nullable = true)
     |-- chc_present: integer (nullable = true)
     |-- chc_short: integer (nullable = true)
    


**Converting Dataframe (df2) to Pandas for plotting graph


```python
df2_surplus_pd  = df2_surplus.toPandas()

# df2_shortage = df2_shortage["sub_centres_short"]
df2_shortage_pd = df2_shortage.toPandas()

label1 = [i for i in df2_surplus_pd["state"]]
label2 = [i for i in df2_shortage_pd["state"]]

# label1
# label2
```

**Ploting graph of df2_surplus


```python
index=np.arange(len(label1)) 
plt.bar(index, df2_surplus_pd["chc_short"])  # Or change to df2
plt.xlabel("States",size=15)
plt.ylabel("CHC Centres - Surplus", size=15)
plt.xticks(index, label1, fontsize=15, rotation=90)
plt.yticks(fontsize = 15)
plt.show()

```


![png](output_65_0.png)


**Ploting graph of df2_shortage


```python
index=np.arange(len(label2)) 
plt.bar(index, df2_shortage_pd["chc_short"])  # Or change to df2
plt.xlabel("States",size=15)
plt.ylabel("CHC - Shortage", size=15)
plt.xticks(index, label2, fontsize=15, rotation=90)
plt.yticks(fontsize = 15)
plt.show()
```


![png](output_67_0.png)

