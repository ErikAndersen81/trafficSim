FROM python:3
WORKDIR /usr/src/app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000

ENV FLASK_APP "server.py"
ENV FLASK_DEBUG "True"
ENV FLASK_ENV "development"

CMD ["python", "./server.py"]