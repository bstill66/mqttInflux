# MQTT Parameters
ARG MQTT_URL="104.53.51.51"
ARG MQTT_USER="delta"
ARG MQTT_PASSWORD="KeepClimbing!"

ARG INFLUX_SERVER="http://104.53.51.51:8086"
ARG INFLUX_APIKEY

# Directories used by application
ARG ROOT=""
ARG APP_DIR="${ROOT}/app"
ARG LOG_DIR="${ROOT}/logs"
ARG APP_USER="server"


FROM python:3.12-alpine AS BASE

LABEL authors="Brian Still"

RUN pip install --upgrade pip
RUN pip install python-dateutil \
                paho-mqtt

# Directories used by application
ARG APP_DIR
ARG LOG_DIR

# MQTT Parameters
ARG MQTT_URL
ARG MQTT_USER
ARG MQTT_PASSWORD
ARG APP_USER

ARG INFLUX_SERVER
ARG INFLUX_APIKEY



RUN adduser -D -g '' ${APP_USER}


RUN mkdir -p ${APP_DIR}
RUN mkdir -p ${LOG_DIR}

WORKDIR ${APP_DIR}

# Copy in the common code
RUN mkdir -p ${APP_DIR}/common
COPY ./common/*.py ${APP_DIR}/common/


# Install the application dependencies/logs
RUN pip install  influxdb_client


# Copy in the client source code
COPY ./server/*.py  ${APP_DIR}/server/


USER ${APP_USER}
ENV MQTT_USER=${MQTT_USER}
ENV MQTT_PASSWORD=${MQTT_PASSWORD}
ENV MQTT_URL=${MQTT_URL}
ENV LOG_DIR=${LOG_DIR}
ENV APP_DIR=${APP_DIR}

ENV INFLUX_SERVER=${INFLUX_SERVER}
ENV INFLUX_APIKEY=${INFLUX_APIKEY}

ENV PYTHONPATH=${APP_DIR}

CMD "python3" "${APP_DIR}/server/InfluxMqttServer.py" \
              "-b" "${MQTT_URL}"               \
              "-L" "${LOG_DIR}"                \
              "--user" "${MQTT_USER}"          \
              "--password" "${MQTT_PASSWORD}"  \
              "-i" "${INFLUX_SERVER}"


