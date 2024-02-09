# mqtt-gateway

This is an example for a gateway component which receives MQTT messages from 
* [OpenDTU](https://github.com/tbnobody/OpenDTU)
* Shelly (Generic status update)
* Sensor data ([Klimalogger](https://github.com/wuan/klimalogger))

and writes the data into InfluxDB / TimescaleDB time series databases.

## Example configuration

File `config.yml` in root folder:

```yaml
mqttUrl: "mqtt://<hostname>:1883"
sources:
  - name: "Sensor data"
    type: "sensor"
    prefix: "sensors"
    targets:
      - type: "influxdb"
        host: "<influx host>"
        port: 8086
        database: "sensors"
      - type: "postgresql"
        host: "<postgres host>"
        port: 5432
        user: "<psql username>"
        password: "<psql password"
        database: "sensors"
  - name: "Shelly data"
    type: "shelly"
    prefix: "shellies"
    targets:
      - type: "influxdb"
        host: "<influx host>"
        port: 8086
        database: "shelly"
      - type: "postgresql"
        host: "<postgres host>"
        port: 5433
        user: "<psql username>"
        password: "<psql password>"
        database: "shelly"
  - name: "PV data"
    type: "opendtu"
    prefix: "solar"
    targets:
      - type: "influxdb"
        host: "<influx host>"
        port: 8086
        database: "solar"

```