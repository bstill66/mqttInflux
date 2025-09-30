# MQTT Parameters
ARG MQTT_URL="104.53.51.51"
ARG MQTT_USER
ARG MQTT_PASSWORD
ARG MSI_HOST="https://msi.viasat.com:9100/v1/flight"


# Directories used by application
ARG ROOT="/"
ARG APP_DIR="${ROOT}app"
ARG LOG_DIR="${ROOT}logs"
ARG APP_USER="client"
ARG CFG_DIR="${ROOT}config"

FROM python:3.12-alpine AS base
ARG APP_USER
ARG APP_DIR
ARG LOG_DIR
ARG MQTT_URL
ARG MQTT_USER
ARG MQTT_PASSWORD
ARG CFG_DIR
ARG MSI_HOST

LABEL Author="Brian Still"
LABEL Description="ViaSat MSI API Collector to MQTT broker"

RUN pip install --upgrade pip
RUN pip install python-dateutil \
                paho-mqtt boto3




RUN pip install requests \
                urllib3

RUN adduser -D -g '' ${APP_USER}


RUN mkdir -p ${APP_DIR}
RUN mkdir -p ${LOG_DIR}
RUN mkdir -p "${CFG_DIR}"

WORKDIR ${APP_DIR}


# Install the application dependencies/logs
RUN pip install --upgrade pip
RUN pip install requests \
                urllib3


# Copy in the client source code
COPY ./client/*.py  ${APP_DIR}/client/

# Copy in the common code
RUN mkdir ${APP_DIR}/common
COPY ./common/MqttClient.py     ${APP_DIR}/common/MqttClient.py
COPY ./common/Client.py         ${APP_DIR}/common/Client.py
COPY ./common/KinesisClient.py  ${APP_DIR}/common/KinesisClient.py

USER ${APP_USER}

ENV MQTT_USER=${MQTT_USER}
ENV MQTT_PASSWORD=${MQTT_PASSWORD}
ENV MQTT_URL=${MQTT_URL}
ENV APP_DIR=${APP_DIR}
ENV LOG_DIR=${LOG_DIR}
ENV CFG_DIR=${CFG_DIR}
ENV MSI_HOST=${MSI_HOST}

ENV PYTHONPATH="${APP_DIR}"
CMD "python3" "${APP_DIR}/client/AircraftClient.py"  \
              "-b" "${MQTT_URL}" \
              "-L" "${LOG_DIR}"  \
              "-u" "${MQTT_USER}" \
              "-p" "${MQTT_PASSWORD}" \
              "-M" "${MSI_HOST}" \
              "-K" "${CFG_DIR}/DalKinesis.cfg"
