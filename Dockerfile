FROM quay.io/astronomer/astro-runtime:12.2.0


RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt_project/dbt-requirements.txt  && \
    cd dbt_project && dbt deps && cd .. && \
    deactivate