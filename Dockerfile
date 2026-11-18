FROM python:3.12-slim
WORKDIR /app
COPY faultgrid/ faultgrid/
COPY tests/ tests/
COPY README.md results.json ./
RUN pip install --no-cache-dir pytest
CMD ["sh", "-c", "python -m pytest tests/ -q && python -m faultgrid.bench"]
