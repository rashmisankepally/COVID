FROM python:3

RUN pip install --upgrade pip
RUN pip install tweepy
RUN pip install --upgrade google-api-python-client
RUN pip install --upgrade oauth2client
RUN pip install python-dateutil
RUN pip install --upgrade google-cloud

ADD twitter_to_pubsub.py /twitter_to_pubsub.py
ADD utils.py /utils.py

CMD python twitter_to_pubsub.py
