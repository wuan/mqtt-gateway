# mqtt-gateway

This is an example for a gateway component which receives MQTT messages from 
* OpenDTU
* Shelly (Generic status update)
* Sensor data (Klimalooger)

and writes the data into InfluxDB / TimescaleDB time series databases.
