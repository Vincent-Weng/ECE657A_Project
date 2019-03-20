# ECE657A_Project

Usage for Daily Data retrieval:

```bash
spark-submit --class ca.uwaterloo.ece657a.weather.DailyData target/datacleaning-1.0.jar --factor temperature --city Vancouver
```

Usage for Predict (hourly) Data retrieval:

```bash
spark-submit --class ca.uwaterloo.ece657a.weather.PredictData target/datacleaning-1.0.jar --city Vancouver
```



