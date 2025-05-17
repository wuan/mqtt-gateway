[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=wuan_mqtt-gateway&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=wuan_mqtt-gateway)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=wuan_mqtt-gateway&metric=coverage)](https://sonarcloud.io/summary/new_code?id=wuan_mqtt-gateway)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=wuan_mqtt-gateway&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=wuan_mqtt-gateway)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=wuan_mqtt-gateway&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=wuan_mqtt-gateway)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/wuan/mqtt-gateway/badge)](https://scorecard.dev/viewer/?uri=github.com/wuan/mqtt-gateway)

# mqtt-gateway

This is an example for a gateway component which receives MQTT messages from 
* [OpenDTU](https://github.com/tbnobody/OpenDTU)
* [OpenMQTTGateway](https://github.com/1technophile/OpenMQTTGateway)
* Shelly (Generic status update)
* Sensor data ([Klimalogger](https://github.com/wuan/klimalogger), [CircuitPy-Logger](https://github.com/wuan/circuitpy-logger))

and writes the data into InfluxDB / TimescaleDB (PostgreSQL) time series databases.

## Example configuration

File `config.yml` in root folder:

```yaml
mqttUrl: "mqtt://<hostname>:1883"
mqttClientId: "sensors_gateway"
sources:
  - name: "Sensor data"
    type: "sensor"
    prefix: "sensors"
    targets:
      - type: "influxdb"
        url: "http://<host>:8086"
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
