FROM debian:11-slim AS build-stage
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-venv \
    gcc \
    libpython3-dev && \
    python3 -m venv /venv && \
    /venv/bin/pip install --upgrade pip setuptools wheel

FROM build-stage AS build-venv
COPY ./python/requirements.txt /requirements.txt
RUN /venv/bin/pip install --disable-pip-version-check -r /requirements.txt


FROM gcr.io/distroless/python3-debian11:debug AS release
COPY --from=build-venv /venv /venv
COPY ./python /app/python
WORKDIR /app
RUN mkdir /app/logs && \
    chown -R nonroot:nonroot /app
    
USER nonroot:nonroot

ENTRYPOINT ["tail", "-f", "/dev/null"]