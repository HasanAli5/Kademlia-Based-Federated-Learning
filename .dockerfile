FROM pytorch/pytorch:2.11.0-cuda13.0-cudnn9-runtime

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY . .

RUN mkdir ./data
RUN mkdir ./results

CMD [ "sleep",'infinity' ]