[![Lines of Code](https://sonar.tryb.de/api/project_badges/measure?project=mqtt-gateway&metric=ncloc&token=sqb_dc05ae645d8c0daa80bfaf9959513149ce8572fe)](https://sonar.tryb.de/dashboard?id=mqtt-gateway)
[![Duplicated Lines (%)](https://sonar.tryb.de/api/project_badges/measure?project=mqtt-gateway&metric=duplicated_lines_density&token=sqb_dc05ae645d8c0daa80bfaf9959513149ce8572fe)](https://sonar.tryb.de/dashboard?id=mqtt-gateway)
[![Coverage](https://sonar.tryb.de/api/project_badges/measure?project=mqtt-gateway&metric=coverage&token=sqb_dc05ae645d8c0daa80bfaf9959513149ce8572fe)](https://sonar.tryb.de/dashboard?id=mqtt-gateway)
[![Technical Debt](https://sonar.tryb.de/api/project_badges/measure?project=mqtt-gateway&metric=sqale_index&token=sqb_dc05ae645d8c0daa80bfaf9959513149ce8572fe)](https://sonar.tryb.de/dashboard?id=mqtt-gateway)

# mqtt-gateway

This is an example for a gateway component which receives MQTT messages from 
* [OpenDTU](https://github.com/tbnobody/OpenDTU)
* Shelly (Generic status update)
* Sensor data ([Klimalogger](https://github.com/wuan/klimalogger), [CircuitPy-Logger](https://github.com/wuan/circuitpy-logger))

and writes the data into InfluxDB / TimescaleDB time series databases.

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