# ECE657A_Project

**Usage for Daily Data retrieval:**

**Output:** `<day>, <value>`

**Parameters:**

`--factor`: the factor that will be retrieved.

`--city`: the city to retrieve data for

```bash
spark-submit --class ca.uwaterloo.ece657a.weather.DailyData target/datacleaning-1.0.jar --factor temperature --city Vancouver
```

**Usage for Predict (hourly) Data retrieval:**

**Output:** `<day>, <hour>, <temperature>, <humidity>, <pressure>, <wind_speed>, <weather_description>`

**Parameters:**

`--accurate`: if provided it will generate accuate weather description, for example:

`2012-10-02,15,11.700006103515648,89.0,1027.0,0.0,sky is clear`

otherwise like this:

`2012-10-02,15,11.700006103515648,89.0,1027.0,0.0,clear`

```bash
spark-submit --class ca.uwaterloo.ece657a.weather.PredictData target/datacleaning-1.0.jar --city Vancouver
spark-submit --class ca.uwaterloo.ece657a.weather.PredictData target/datacleaning-1.0.jar --city Vancouver --accurate
```



