FROM python:3.6

ADD . .
ADD scripts /usr/local/src/scripts
ADD requirements /usr/local/src/requirements

WORKDIR /usr/local/src/scripts

RUN apt-get update && apt-get install -y vim
RUN pip install --upgrade -r /usr/local/src/requirements/requirements.txt
