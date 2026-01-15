FROM postgres:15

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN pip3 install --no-cache-dir --break-system-packages psycopg2-binary

RUN python3 --version && pip3 list | grep psycopg2
