# OpenDTU support

## Exmaple data

```
original "solar/ac/power", "0.0"
original "solar/ac/yieldtotal", "13.338"
original "solar/ac/yieldday", "216"
original "solar/ac/is_valid", "0"
original "solar/dc/power", "0.6"
original "solar/dc/irradiation", "0.030"
original "solar/dc/is_valid", "0"
original "solar/dtu/status", "online"
original "solar/dtu/uptime", "2125105"
original "solar/dtu/ip", "192.168.110.90"
original "solar/dtu/hostname", "OpenDTU-07F614"
original "solar/dtu/rssi", "-74"
original "solar/dtu/bssid", "7A:45:58:94:4B:CA"
original "solar/114190641177/name", "HM-800"
original "solar/114190641177/status/reachable", "0"
original "solar/114190641177/status/producing", "0"
original "solar/114190641177/status/last_update", "1701271852"
original "solar/114190641177/status/limit_relative", "75.00"
original "solar/114190641177/status/limit_absolute", "600.00"
original "solar/114190641177/0/powerdc", "0.6"
  inverter: powerdc: "0.6"
   -> WriteQuery { fields: [("value", Float(0.6))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "powerdc", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/yieldday", "216"
  inverter: yieldday: "216"
   -> WriteQuery { fields: [("value", Float(216.0))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "yieldday", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/yieldtotal", "13.338"
  inverter: yieldtotal: "13.338"
   -> WriteQuery { fields: [("value", Float(13.338))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "yieldtotal", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/voltage", "229.0"
  inverter: voltage: "229.0"
   -> WriteQuery { fields: [("value", Float(229.0))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "voltage", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/current", "0.00"
  inverter: current: "0.00"
   -> WriteQuery { fields: [("value", Float(0.0))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "current", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/power", "0.0"
  inverter: power: "0.0"
   -> WriteQuery { fields: [("value", Float(0.0))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "power", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/frequency", "50.00"
  inverter: frequency: "50.00"
   -> WriteQuery { fields: [("value", Float(50.0))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "frequency", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/powerfactor", "0.000"
  inverter: powerfactor: "0.000"
   -> WriteQuery { fields: [("value", Float(0.0))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "powerfactor", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/efficiency", "0.000"
  inverter: efficiency: "0.000"
   -> WriteQuery { fields: [("value", Float(0.0))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "efficiency", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/reactivepower", "0.0"
  inverter: reactivepower: "0.0"
   -> WriteQuery { fields: [("value", Float(0.0))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "reactivepower", timestamp: Seconds(1701271852) }
original "solar/114190641177/0/temperature", "7.7"
  inverter: temperature: "7.7"
   -> WriteQuery { fields: [("value", Float(7.7))], tags: [("device", Text("114190641177")), ("component", Text("inverter")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "temperature", timestamp: Seconds(1701271852) }
original "solar/114190641177/1/voltage", "14.1"
  string 1: voltage: "14.1"
   -> WriteQuery { fields: [("value", Float(14.1))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("1")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "voltage", timestamp: Seconds(1701271852) }
original "solar/114190641177/1/current", "0.02"
  string 1: current: "0.02"
   -> WriteQuery { fields: [("value", Float(0.02))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("1")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "current", timestamp: Seconds(1701271852) }
original "solar/114190641177/1/power", "0.2"
  string 1: power: "0.2"
   -> WriteQuery { fields: [("value", Float(0.2))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("1")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "power", timestamp: Seconds(1701271852) }
original "solar/114190641177/1/yieldday", "74"
  string 1: yieldday: "74"
   -> WriteQuery { fields: [("value", Float(74.0))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("1")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "yieldday", timestamp: Seconds(1701271852) }
original "solar/114190641177/1/yieldtotal", "7.664"
  string 1: yieldtotal: "7.664"
   -> WriteQuery { fields: [("value", Float(7.664))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("1")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "yieldtotal", timestamp: Seconds(1701271852) }
original "solar/114190641177/1/irradiation", "0.017"
  string 1: irradiation: "0.017"
   -> WriteQuery { fields: [("value", Float(0.017))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("1")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "irradiation", timestamp: Seconds(1701271852) }
original "solar/114190641177/2/voltage", "14.1"
  string 2: voltage: "14.1"
   -> WriteQuery { fields: [("value", Float(14.1))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("2")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "voltage", timestamp: Seconds(1701271852) }
original "solar/114190641177/2/current", "0.03"
  string 2: current: "0.03"
   -> WriteQuery { fields: [("value", Float(0.03))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("2")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "current", timestamp: Seconds(1701271852) }
original "solar/114190641177/2/power", "0.4"
  string 2: power: "0.4"
   -> WriteQuery { fields: [("value", Float(0.4))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("2")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "power", timestamp: Seconds(1701271852) }
original "solar/114190641177/2/yieldday", "142"
  string 2: yieldday: "142"
   -> WriteQuery { fields: [("value", Float(142.0))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("2")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "yieldday", timestamp: Seconds(1701271852) }
original "solar/114190641177/2/yieldtotal", "5.674"
  string 2: yieldtotal: "5.674"
   -> WriteQuery { fields: [("value", Float(5.674))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("2")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "yieldtotal", timestamp: Seconds(1701271852) }
original "solar/114190641177/2/irradiation", "0.050"
  string 2: irradiation: "0.050"
   -> WriteQuery { fields: [("value", Float(0.05))], tags: [("device", Text("114190641177")), ("component", Text("string")), ("string", Text("2")), ("year", UnsignedInteger(2023)), ("month", UnsignedInteger(11)), ("year_month", Text("2023-11"))], measurement: "irradiation", timestamp: Seconds(1701271852) }
original "solar/114190641177/device/bootloaderversion", "104"
original "solar/114190641177/device/fwbuildversion", "10016"
original "solar/114190641177/device/fwbuilddatetime", "2022-11-10 16:11:00"
original "solar/dtu/uptime", "2125115"
original "solar/dtu/ip", "192.168.110.90"
original "solar/dtu/hostname", "OpenDTU-07F614"
original "solar/dtu/rssi", "-73"
original "solar/dtu/bssid", "7A:45:58:94:4B:CA"
original "solar/114190641177/name", "HM-800"
original "solar/114190641177/device/bootloaderversion", "104"
original "solar/114190641177/device/fwbuildversion", "10016"
original "solar/114190641177/device/fwbuilddatetime", "2022-11-10 16:11:00"
original "solar/114190641177/device/hwpartnumber", "269565952"
original "solar/114190641177/device/hwversion", "01.10"
original "solar/114190641177/status/limit_relative", "75.00"
original "solar/114190641177/status/limit_absolute", "600.00"
original "solar/114190641177/status/reachable", "0"
original "solar/114190641177/status/producing", "0"
original "solar/114190641177/status/last_update", "1701271852"
```