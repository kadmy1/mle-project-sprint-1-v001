# dags/flats_data.py

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    schedule='@once',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def prepare_flat_dataset():
    @task()
    def create_table():
        from sqlalchemy import MetaData, Table, Column, Integer, UniqueConstraint, inspect, Float, Boolean
        metadata = MetaData()
        hook = PostgresHook('destination_db')
        db_conn = hook.get_sqlalchemy_engine()
        flats_table = Table(
            'flats_data',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('flat_id', Integer),
            Column('floor', Integer),
            Column('is_apartment', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('studio', Integer),
            Column('total_area', Float),
            Column('price', Float),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Integer),
            UniqueConstraint('flat_id', name='unique_flat_id_constraint')
        )
        if not inspect(db_conn).has_table(flats_table.name):
            metadata.create_all(db_conn)

    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
            select 
                f.id flat_id,
                coalesce(f.floor, 1) floor, 
                case when f.is_apartment then 1 else 0 end is_apartment, 
                coalesce(f.kitchen_area, 0) kitchen_area, 
                coalesce(f.living_area, f.total_area - f.kitchen_area, 0) living_area, 
                coalesce(f.rooms, 1) rooms, 
                case when f.studio then 1 else 0 end studio, 
                coalesce(f.total_area, t.avg_area) total_area,
                coalesce(f.price, t.avg_price) price,
                coalesce(b.build_year, 2024) build_year, 
                coalesce(b.building_type_int, tt.building_type_modal) building_type_int, 
                coalesce(b.latitude, avg_latitude) latitude,
                coalesce(b.longitude, avg_longitude) longitude,
                coalesce(b.ceiling_height, avg_ceiling_height) ceiling_height,
                coalesce(b.flats_count, 1) flats_count,
                coalesce(b.floors_total, f.floor, 1) floors_total,
                case when b.has_elevator then 1 else 0 end has_elevator
            from flats f
            inner join buildings b on b.id = f.building_id
            inner join (select 
                            avg(total_area) avg_area, 
                            avg(price) avg_price
                        from flats) t on 1=1
            inner join (select 
                            mode() WITHIN GROUP (ORDER BY building_type_int) building_type_modal, 
                            avg(latitude) avg_latitude,
                            avg(longitude) avg_longitude,
                            avg(ceiling_height) avg_ceiling_height
                        from buildings) tt on 1=1	
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        # at sql
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="flats_data",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
        )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


prepare_flat_dataset()
