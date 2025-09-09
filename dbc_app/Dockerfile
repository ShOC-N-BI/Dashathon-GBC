FROM python:3.12-slim
WORKDIR /dbc_app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8070
CMD ["python", "app.py"]         # adjust to your actual entrypoint
