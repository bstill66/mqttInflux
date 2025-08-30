#!/usr/bin/env bash
source /home/bstill/Cockpit/.venv/bin/activate
python3 /home/bstill/acServer/server/InfluxMqttServer.py -b localhost -u delta -p "KeepClimbing!"