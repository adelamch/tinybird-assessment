FROM python:3.13-slim

WORKDIR /app

COPY Pipfile Pipfile.lock ./
RUN pip install pipenv && pipenv install --system --deploy

COPY . .

ENTRYPOINT ["python", "-m", "src.main"]
