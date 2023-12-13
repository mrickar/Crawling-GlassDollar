FROM python:3-slim
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt 
COPY app/*.py /app/
EXPOSE 8000