# MQTT Parameters
ARG MQTT_URL="localhost"
ARG MQTT_USER
ARG MQTT_PASSWORD
ARG INFLUX_SERVER="http://localhost:8086"


# Directories used by application
ARG ROOT="/"
ARG APP_DIR="${ROOT}app"
ARG LOG_DIR="${ROOT}logs"
ARG APP_USER="server"
ARG CFG_DIR="${ROOT}config"

FROM python:3.12-alpine AS base
ARG APP_USER
ARG APP_DIR
ARG LOG_DIR
ARG MQTT_URL
ARG MQTT_USER
ARG MQTT_PASSWORD
ARG CFG_DIR
ARG INFLUX_SERVER

LABEL Author="Brian Still"
LABEL Description="InfluxDB ingest from aircraft"

RUN pip install --upgrade pip
RUN pip install python-dateutil \
                paho-mqtt boto3




RUN pip install requests \
                urllib3

RUN adduser -D -g  '' ${APP_USER}


RUN mkdir -p ${APP_DIR}
RUN mkdir -p ${LOG_DIR}
RUN mkdir -p ${CFG_DIR}

WORKDIR ${APP_DIR}


# Install the application dependencies/logs
RUN pip install --upgrade pip
RUN pip install influxdb_client


# Copy in the client source code
COPY ./server/*.py  ${APP_DIR}/server/

# Copy in the common code
RUN mkdir ${APP_DIR}/common
COPY ./common/MqttClient.py     ${APP_DIR}/common/MqttClient.py
COPY ./common/Client.py         ${APP_DIR}/common/Client.py
COPY ./common/KinesisClient.py  ${APP_DIR}/common/KinesisClient.py
COPY ./common/MsiToOcc.py       ${APP_DIR}/common/MsiToOcc.py

USER ${APP_USER}

ENV MQTT_USER=${MQTT_USER}
ENV MQTT_PASSWORD=${MQTT_PASSWORD}
ENV MQTT_URL=${MQTT_URL}
ENV APP_DIR=${APP_DIR}
ENV LOG_DIR=${LOG_DIR}
ENV CFG_DIR=${CFG_DIR}
ENV INFLUX_SERVER=${INFLUX_SERVER}


ENV PYTHONPATH="${APP_DIR}"
CMD "python3" "${APP_DIR}/server/InfluxMqttServer.py"  \
              "-b" "${MQTT_URL}" \
              "-L" "${LOG_DIR}"  \
              "-u" "${MQTT_USER}" \
              "-p" "${MQTT_PASSWORD}" \
              "-i" "${INFLUX_SERVER}" \
              "-K" "${CFG_DIR}/DalKinesis.cfg"
