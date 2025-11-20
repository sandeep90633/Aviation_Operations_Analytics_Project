FROM quay.io/astronomer/astro-runtime:12.2.0

RUN python -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake
ENV PATH="/opt/dbt_venv/bin:$PATH"