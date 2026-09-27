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
mqttUrl: "mqtts://<hostname>:8883"   # use mqtt:// for an unencrypted connection
mqttClientId: "sensors_gateway"
mqttUsername: "${MQTT_USERNAME}"     # optional
mqttPassword: "${MQTT_PASSWORD}"     # optional
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
        password: "${POSTGRES_PASSWORD}"
        database: "sensors"
        tls: false
  - name: "Shelly data"
    type: "shelly"
    prefix: "shellies"
    targets:
      - type: "influxdb"
        url: "http://<influx host>:8086"
        database: "shelly"
        token: "${INFLUX_TOKEN}"
      - type: "postgresql"
        host: "<postgres host>"
        port: 5433
        user: "<psql username>"
        password: "${POSTGRES_PASSWORD}"
        database: "shelly"
  - name: "PV data"
    type: "opendtu"
    prefix: "solar"
    targets:
      - type: "influxdb"
        url: "http://<influx host>:8086"
        database: "solar"

```

### Configuration notes

- **MQTT broker**: `mqttUrl`, `mqttClientId` and optional `mqttUsername` /
  `mqttPassword`. A secure scheme (`ssl://`, `tls://`, `mqtts://` or `wss://`)
  enables TLS with server-certificate verification.
- **InfluxDB targets** take a `url` (for example `http://<host>:8086`) and either
  `user`/`password` or a `token`.
- **PostgreSQL targets** are only supported for `sensor` and `shelly` sources,
  because only those provide the required `location`/`sensor`/`value` schema.
  Set `tls: true` to require a verified TLS connection.
- **Environment variables**: any `${VAR_NAME}` placeholder in the configuration
  file is replaced with the value of that environment variable. Referencing an
  undefined variable is a configuration error. This lets secrets stay out of the
  file (for example via Docker/Compose secrets).
