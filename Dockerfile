FROM apache/airflow:slim-3.3.2@sha256:881c35bfb1f6b76d233dd402b7228b5c37003702068b865db5db76061458b2bd
COPY requirements.txt /tmp/paper-requirements.txt
RUN pip install --no-cache-dir -r /tmp/paper-requirements.txt
COPY paper_processing /opt/paper-library/paper_processing
ENV PYTHONPATH=/opt/paper-library
