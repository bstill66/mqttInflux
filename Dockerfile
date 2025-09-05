ARG ROOT="/"
ARG APPDIR="${ROOT}/app"
ARG MQTT_URL="104.53.51.51"
ARG MSI_URL="http://msi.viasat.com:9100/v1/flight"
ARG LOG_DIR="${ROOT}/logs"



FROM python:3.12-alpine

LABEL authors="Brian Still"
RUN adduser -D -g '' ClientUser


RUN mkdir -p /app
RUN mkdir -p /logs

WORKDIR /app


# Install the application dependencies/logs
RUN pip install --upgrade pip
RUN pip install python-dateutil \
                requests \
                urllib3 \
                paho-mqtt


# Copy in the source code
COPY ./client/ /app/client/
RUN mkdir server
COPY ./server/MqttClient.py /app/server/MqttClient.py

USER ClientUser
ENV PYTHONPATH "/app"
CMD ["python3", "/app/client/AircraftClient.py", \
     "-M","https://msi.viasat.com:9100/v1/flight", \
     "-b","104.53.51.51", \
     "-l","/logs", \
     "-A","N830NW" \
     ]