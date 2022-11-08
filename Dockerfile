FROM python:3.8-slim
LABEL maintainer "Si Wang"

COPY requirements_main.txt .
RUN apt-get -y --allow-releaseinfo-change update \
	&& apt-get -y install python3 git \
	&& python3 -m pip install -U pip \
	&& pip3 install -r /requirements_main.txt

COPY application /application

WORKDIR /application

ENTRYPOINT [ "python3", "-u", "main.py" ]