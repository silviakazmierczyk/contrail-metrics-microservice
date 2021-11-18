FROM python:alpine3.14

USER root
#LABEL authors="ogonzalezm@viewnext.com"
LABEL version="1.1"

COPY .  /
RUN cd / && pip install -r requirements.txt

ENV CONTRAILMETRICS2KAFKA_CONFIG /contrailmetrics2kafka.yml
#ENTRYPOINT ["python"]
CMD ["python", "./apireq.py"]