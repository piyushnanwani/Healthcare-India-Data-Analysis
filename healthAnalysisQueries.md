### (1)List of states that have  shortage of sub_centres , phc_centres and chc centres 

INPUT
~~~
check = spark.sql("select state,sub_centres_short,phc_short,chc_short  from df1_table where sub_centres_short>0 and phc_short>0 and chc_short>0 ")
~~~
OUTPUT
~~~
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
~~~

### (2) States with no shortage of sub_centres,phc,chc
INPUT
~~~
check2 = spark.sql("select state from df1_table where sub_centres_short==0 and phc_short==0 and chc_short==0 ")
~~~
OUTPUT
~~~
+--------------------+
|               state|
+--------------------+
|                 Goa|
|    Himachal Pradesh|
|              Kerala|
|             Mizoram|
|Andaman & Nicobar...|
|          Chandigarh|
|         Daman & Diu|
|         Lakshadweep|
+--------------------+
~~~

### (3) Top 5 most populated states
INPUT
~~~
check3 = spark.sql("select state,population from df1_table order by population desc limit 5")
~~~
OUTPUT
~~~
+--------------+----------+
|         state|population|
+--------------+----------+
| Uttar Pradesh| 155111022|
|         Bihar|  92075028|
|   West Bengal|  62213676|
|   Maharashtra|  61545441|
|Andhra Pradesh|  56311788|
+--------------+----------+
~~~

### (4) State with maximum tribal population

INPUT
~~~
check4 = spark.sql("select state,tribal_population from df1_table order by tribal_population desc limit 1")
~~~
OUTPUT
~~~
+--------------+-----------------+
|         state|tribal_population|
+--------------+-----------------+
|Madhya Pradesh|         13550258|
+--------------+-----------------+

~~~

### (5)all details of  Lakshadweep
INPUT
~~~
check5 = spark.sql("select *  from df1_table where state='Lakshadweep'")
~~~
OUTPUT
~~~
+---+-----------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
| sn|      state|population|tribal_population|sub_centres_req|sub_centres_present|sub_centres_short|phc_req|phc_present|phc_short|chc_req|chc_present|chc_short|
+---+-----------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+
| 34|Lakshadweep|     14121|            13503|              4|                 14|                0|      0|          4|        0|      0|          3|        0|
+---+-----------+----------+-----------------+---------------+-------------------+-----------------+-------+-----------+---------+-------+-----------+---------+

~~~
