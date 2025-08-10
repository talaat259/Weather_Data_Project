#!/bin/bash

echo "*** Starting the Data Pipeline ***"

# Open one GNOME Terminal with multiple tabs
gnome-terminal \
  --tab --title="Zookeeper" -- bash -c "cd ~/kafka_2.13-3.5.1 && bin/zookeeper-server-start.sh config/zookeeper.properties; exec bash" \
  --tab --title="Kafka Broker" -- bash -c "sleep 5 && cd ~/kafka_2.13-3.5.1 && bin/kafka-server-start.sh config/server.properties; exec bash" \
  --tab --title="Extractors" -- bash -c "sleep 10 && cd ~/De_Project && python3 Extractors.py; exec bash" \
  --tab --title="Producer" -- bash -c "sleep 12 && cd ~/De_Project && python3 Client.py; exec bash" \
  --tab --title="Consumer" -- bash -c "sleep 14 && cd ~/De_Project && python3 Temp_weather_app.py; exec bash"
echo "*** Data Pipeline Started Successfully ***"