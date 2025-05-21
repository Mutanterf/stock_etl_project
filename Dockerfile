FROM apache/airflow:2.7.0

USER root

COPY create_user.sh /create_user.sh
RUN chmod +x /create_user.sh

USER airflow

RUN pip install --user clickhouse-driver dbt-core dbt-clickhouse

ENTRYPOINT ["/create_user.sh"]
CMD ["airflow", "webserver"]
