FROM python:3.10

ENV DEBIAN_FRONTEND noninteractive

# clean-up
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install -r /tmp/requirements.txt


COPY . app
WORKDIR app

