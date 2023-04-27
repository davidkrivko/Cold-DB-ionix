from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
)

MAIN_DB_STRING = "postgresql://doadmin:AVNS_G54Em5z5dQ_roYxMR0-@db-postgresql-nyc1-8384" \
                 "6-do-user-13548984-0.b.db.ondigitalocean.com:25060/defaultdb?sslmode=require"
engine = create_engine(MAIN_DB_STRING)

meta = MetaData()
meta.reflect(bind=engine)

thermostat_table = Table(
    "devices_smartthermostatmodel",
    meta,
    extend_existing=True,
)

boiler_table = Table(
    'devices_boilermodel',
    meta,
    extend_existing=True,
)

building_table = Table(
    'properties_buildingmodel',
    meta,
    extend_existing=True,
)


controller_table = Table(
    'devices_ioniqcontrollermodel',
    meta,
    extend_existing=True,
)


zip_code_table = Table(
    'properties_zipcodemodel',
    meta,
    extend_existing=True,
)


def list_of_thermostats(connection):
    query = thermostat_table.select().with_only_columns(thermostat_table.c.serial_num)

    result_set = connection.execute(query)
    return [row[0] for row in result_set]


def list_of_controller(connection):
    query = controller_table.select().with_only_columns(controller_table.c.serial_num)

    result_set = connection.execute(query)
    return [row[0] for row in result_set]
