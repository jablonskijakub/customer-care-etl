FROM ubuntu:20.04 
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
RUN mkdir /app 
COPY . /app
WORKDIR /app
RUN apt-get update
RUN apt-get  --assume-yes install python3.8
RUN apt-get  --assume-yes install python3-pip
RUN pip3 install pipenv
RUN apt-get install -y libpq-dev
RUN pipenv install 
