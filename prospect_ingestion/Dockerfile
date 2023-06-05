FROM amazoncorretto:8 as build-stage

RUN yum -y update
RUN yum -y install yum-utils
RUN yum -y groupinstall development

RUN yum list python3*
RUN yum -y install python3 python3-dev python3-pip python3-virtualenv git

RUN python -V
RUN python3 -V

ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3

RUN pip3 install --upgrade pip

RUN pip3 install pex pyspark==3.4.0 unidecode email_validator boto3 pandas

RUN mkdir /app
WORKDIR /app

COPY . /app
RUN rm -rf dist

RUN pip3 freeze > requirements.txt

RUN pip3 install -r requirements.txt

RUN pex -r requirements.txt . -v --disable-cache -D prospect_ingestion/docs --ignore-errors -o prospect_ingestion.pex

FROM scratch AS app

COPY --from=build-stage /app/prospect_ingestion.pex .
COPY --from=build-stage /app/requirements.txt .
