import datetime

from postgres import (
    thermostat_table,
    controller_table,
    building_table,
    zip_code_table,
    boiler_table, switch_table,
)

from redis_dir.utils import fetch_online_status_from_online_stream

from sqlalchemy import create_engine

from sqlalchemy import desc
from sqlalchemy import (
    Table,
    Column,
    String,
    DateTime,
    MetaData,
    Numeric,
)
from sqlalchemy.sql.sqltypes import Boolean
from redis_dir.daos import RedisDao

dao = RedisDao()

IOT_DB_NAME = "colddb"
IOT_DB_USER = "admin"
IOT_DB_PASSWORD = "mMFavHYtuA-43LP4MXXdFaQMpdY5ye0l"
IOT_DB_HOST = "localhost"
IOT_DB_PORT = "5432"

THERMOSTAT_QUERY_LIMIT = 1

DB_STRING = f"postgresql://{IOT_DB_USER}:{IOT_DB_PASSWORD}@{IOT_DB_HOST}:{IOT_DB_PORT}/{IOT_DB_NAME}"
db = create_engine(DB_STRING)
meta = MetaData()
meta.reflect(bind=db)

api = Table(
    "api",
    meta,
    Column("id", Numeric, primary_key=True, autoincrement=True),
    Column("user_id", Numeric),
    Column("t_api", String),
    Column("comment", String),
    extend_existing=True,
)

controller_temp_fluctuation = Table(
    "controller_temp_fluctuation",
    meta,
    Column("id", Numeric, primary_key=True, autoincrement=True),
    Column("sn", String),
    Column("sys_temp_1", Numeric),
    Column("outdoor_temp", Numeric),
    Column("timestamp", DateTime),
    Column("sys_temp_2", Numeric),
    Column("call_for_heat", Numeric),
    extend_existing=True,
)

cycles = Table(
    "cycles",
    meta,
    Column("id", Numeric, primary_key=True, autoincrement=True),
    Column("sn", String),
    Column("value", Boolean),
    Column("timestamp", DateTime),
    extend_existing=True,
)

devices = Table(
    "devices",
    meta,
    Column("id", Numeric, primary_key=True, autoincrement=True),
    Column("sn", String),
    Column("basic", Numeric),
    Column("premium", Numeric),
    Column("timestamp", DateTime),
    extend_existing=True,
)

errors = Table(
    "errors",
    meta,
    Column("id", Numeric, primary_key=True, autoincrement=True),
    Column("sn", String),
    Column("error_code", Numeric),
    Column("timestamp", DateTime),
    extend_existing=True,
)

thermostat_temp_fluctuation = Table(
    "thermostat_temp_fluctuation",
    meta,
    Column("id", Numeric, primary_key=True, autoincrement=True),
    Column("sn", String),
    Column("th_temp", Numeric),
    Column("outdoor_temp", Numeric),
    Column("timestamp", DateTime),
    extend_existing=True,
)

user_data = Table(
    "user_data",
    meta,
    Column("id", Numeric, primary_key=True, autoincrement=True),
    Column("address", String),
    Column("serial_number", String),
    Column("user_id", Numeric),
    Column("t_api", String),
    extend_existing=True,
)

setpoint_data = Table(
    "setpoint_data",
    meta,
    Column("id", Numeric, primary_key=True, autoincrement=True),
    Column("sn", String),
    Column("setpoint", Numeric),
    Column("timestamp", DateTime),
    extend_existing=True,
)

# meta.create_all(db)


def cycles_func(serial_num: str, conn_cold, relay):
    cycle_statement = (
        cycles.select()
        .where(cycles.c.sn == serial_num)
        .order_by(desc("timestamp"))
        .limit(THERMOSTAT_QUERY_LIMIT)
    )
    result_set = conn_cold.execute(cycle_statement)
    cycle_data = result_set.fetchall()

    now = datetime.datetime.now()
    if cycle_data:
        latest_record = cycle_data[0]
        if bool(int(relay)) is not bool(latest_record[2]):
            relay_statement = (
                cycles.insert().values(
                    sn=serial_num,
                    value=bool(int(relay)),
                    timestamp=now,
                )
            )
            conn_cold.execute(relay_statement)
            conn_cold.commit()
    else:
        relay_statement = (
            cycles.insert().values(
                sn=serial_num,
                value=bool(int(relay)),
                timestamp=now,
            )
        )
        conn_cold.execute(relay_statement)
        conn_cold.commit()


def online_status_check(serial_num: str, connection_cold, redis_data):
    status = fetch_online_status_from_online_stream(redis_data["timestamp"])

    if not status["data"]:
        try:
            status_statement = (
                errors.select()
                .where(errors.c.sn == serial_num)
                .order_by(desc("timestamp"))
                .limit(THERMOSTAT_QUERY_LIMIT)
            )
            result_set = connection_cold.execute(status_statement)
            rows = result_set.fetchall()
            status_date = rows[0][-1]
            if status_date.date() != datetime.date.today():
                error_statement = errors.insert().values(
                    sn=serial_num,
                    error_code=101,
                    timestamp=datetime.datetime.now()
                )
                connection_cold.execute(error_statement)
                connection_cold.commit()
        except:
            pass


async def thermostat_data(serial_num: str, connection_cold, connection_main, connection_redis) -> None:
    th_data = dao.get_paired_thermostat_data(serial_num, connection_redis)
    now = datetime.datetime.now()

    if th_data:
        online_status_check(serial_num, connection_cold, th_data)

        query = (
            thermostat_table.select()
            .join(boiler_table)
            .join(controller_table)
            .join(building_table)
            .join(zip_code_table)
            .where(thermostat_table.c.serial_num == serial_num)
            .with_only_columns(zip_code_table.c.todays_temp)
        )
        try:
            outdoor_temp = connection_main.execute(query).fetchone()[0]
        except:
            outdoor_temp = None
        try:
            cycles_func(serial_num, connection_cold, th_data["relay"])

            temp_th_statement = (
                thermostat_temp_fluctuation.select()
                .where(thermostat_temp_fluctuation.c.sn == serial_num)
                .order_by(desc("timestamp"))
                .limit(THERMOSTAT_QUERY_LIMIT)
            )
            result_set = connection_cold.execute(temp_th_statement)
            temp_th_data = result_set.fetchall()

            if temp_th_data:
                if temp_th_data[0][2] != int(th_data["t1"]):
                    insert_statement = (
                        thermostat_temp_fluctuation.insert().values(
                            sn=serial_num,
                            th_temp=int(th_data["t1"]),
                            outdoor_temp=outdoor_temp,
                            timestamp=now,
                        )
                    )
                    connection_cold.execute(insert_statement)
                    connection_cold.commit()
            else:
                insert_statement = (
                    thermostat_temp_fluctuation.insert().values(
                        sn=serial_num,
                        th_temp=int(th_data["t1"]),
                        outdoor_temp=outdoor_temp,
                        timestamp=now,
                    )
                )
                connection_cold.execute(insert_statement)
                connection_cold.commit()

            setpoint_statement = (
                setpoint_data.select()
                .where(setpoint_data.c.sn == serial_num)
                .order_by(desc("timestamp"))
                .limit(THERMOSTAT_QUERY_LIMIT)
            )
            result_set = connection_cold.execute(setpoint_statement)
            data = result_set.fetchall()

            if data:
                if data[0][2] != int(th_data["setpoint"]):
                    insert_statement = (
                        setpoint_data.insert().values(
                            sn=serial_num,
                            setpoint=th_data["setpoint"],
                            timestamp=now,
                        )
                    )
                    connection_cold.execute(insert_statement)
                    connection_cold.commit()
            else:
                setpoint_statement = (
                    setpoint_data.insert().values(
                        sn=serial_num,
                        setpoint=int(th_data["setpoint"]),
                        timestamp=now,
                    )
                )
                connection_cold.execute(setpoint_statement)
                connection_cold.commit()
        except:
            pass


async def controller_data(serial_num: str, conn_cold, conn_main, connection_redis) -> None:
    now = datetime.datetime.now()
    data = []
    ctr_data = dao.get_paired_relay_controller_data(serial_num, connection_redis)

    if ctr_data:
        cycles_func(serial_num, conn_cold,
                    ctr_data["relay"] if ctr_data.get("relay") is not None else ctr_data["endswitch"])

        online_status_check(serial_num, conn_cold, ctr_data)

        query = (
            controller_table.select()
            .join(building_table)
            .join(zip_code_table)
            .where(controller_table.c.serial_num == serial_num)
            .with_only_columns(zip_code_table.c.todays_temp)
        )
        try:
            outdoor_temp = conn_main.execute(query).fetchone()[0]
        except:
            outdoor_temp = None

        try:
            select_statement = (
                controller_temp_fluctuation.select()
                .where(controller_temp_fluctuation.c.sn == serial_num)
                .order_by(desc("timestamp"))
                .limit(THERMOSTAT_QUERY_LIMIT)
            )
            result_set = conn_cold.execute(select_statement)
            for r in result_set.all():
                data.append(r)
            try:
                relay = int(ctr_data["relay"])
            except:
                relay = int(ctr_data["endswitch"])
            if data:
                latest_record = data[0]
                if int(ctr_data["t1"]) != latest_record[2]:
                    insert_statement = (
                        controller_temp_fluctuation.insert().values(
                            sn=serial_num,
                            sys_temp_1=int(ctr_data["t1"]),
                            outdoor_temp=outdoor_temp,
                            timestamp=now,
                            sys_temp_2=int(ctr_data["t2"]),
                            call_for_heat=relay,
                        )
                    )
                    conn_cold.execute(insert_statement)
                    conn_cold.commit()
            else:
                insert_statement = (
                    controller_temp_fluctuation.insert().values(
                        sn=serial_num,
                        sys_temp_1=int(ctr_data["t1"]),
                        outdoor_temp=outdoor_temp,
                        timestamp=now,
                        sys_temp_2=int(ctr_data["t2"]),
                        call_for_heat=relay,
                    )
                )
                conn_cold.execute(insert_statement)
                conn_cold.commit()
        except:
            pass


async def receiver_data(serial_num: str, conn_cold, connection_redis) -> None:
    res_data = dao.get_receiver_data(serial_num, connection_redis)

    if res_data:
        online_status_check(serial_num, conn_cold, res_data)

        try:
            cycles_func(serial_num, conn_cold, res_data)
        except:
            pass


async def switch_data(serial_num: str, conn_cold, conn_main) -> None:
    query = (
        switch_table.select()
        .where(switch_table.c.serial_num == serial_num)
        .with_only_columns(switch_table.c.status)
    )
    try:
        status = conn_main.execute(query).fetchone()[0]
    except:
        return

    cycles_func(serial_num, conn_cold, status)


# async def change_setpoint(serial_num: str) -> None:
#     th_data = dao.get_paired_thermostat_data(serial_num)
#
#     logging.error(f"{serial_num}")
#     if th_data:
#         try:
#             with db.connect() as conn:
#             setpoint_statement = (
#                 setpoint_data.select()
#                 .where(setpoint_data.c.sn == serial_num)
#                 .order_by(desc("timestamp"))
#                 .limit(THERMOSTAT_QUERY_LIMIT)
#             )
#             result_set = conn.execute(setpoint_statement)
#             data = result_set.fetchall()
#
#             if data:
#                 if data[0][2] != int(th_data["setpoint"]):
#                     insert_statement = (
#                         setpoint_data.insert().values(
#                             sn=serial_num,
#                             setpoint=th_data["setpoint"],
#                             timestamp=now,
#                         )
#                     )
#                     conn.execute(insert_statement)
#                     conn.commit()
#             else:
#                 setpoint_statement = (
#                     setpoint_data.insert().values(
#                         sn=serial_num,
#                         setpoint=int(th_data["setpoint"]),
#                         timestamp=now,
#                     )
#                 )
#                 conn.execute(setpoint_statement)
#                 conn.commit()
#         except Exception as e:
#             logging.error(f"Unable to fetch setpoint thermostat data from {serial_num} IoT db: {e}")


# async def cycles_of_relay(serial_num: str) -> None:
#     th_data = dao.get_paired_thermostat_data(serial_num)
#
#     if th_data:
#         try:
#             with db.connect() as conn:
#                 select_statement = (
#                     cycles.select()
#                     .where(cycles.c.sn == serial_num)
#                     .order_by(desc("timestamp"))
#                     .limit(THERMOSTAT_QUERY_LIMIT)
#                 )
#                 result_set = conn.execute(select_statement)
#                 data = result_set.fetchall()
#
#                 if data:
#                     latest_record = data[0]
#                     if bool(int(th_data["relay"])) is not bool(latest_record[2]):
#                         insert_statement = (
#                             cycles.insert().values(
#                                 sn=serial_num,
#                                 value=bool(int(th_data["relay"])),
#                                 timestamp=now,
#                             )
#                         )
#                         conn.execute(insert_statement)
#                         conn.commit()
#                 else:
#                     insert_statement = (
#                         cycles.insert().values(
#                             sn=serial_num,
#                             value=bool(int(th_data["relay"])),
#                             timestamp=now,
#                         )
#                     )
#                     conn.execute(insert_statement)
#                     conn.commit()
#         except Exception as e:
#             logging.error(f"Unable to fetch relay from {serial_num} IoT db: {e}")
# async def change_indoor_temp(serial_num: str) -> None:
#     th_data = dao.get_paired_thermostat_data(serial_num)
#
#     try:
#         with engine.connect() as conn:
#             outdoor_temp = conn.execute(query).fetchone()[0]
#     except Exception as e:
#         logging.error(f"Unable to fetch temperature data from {serial_num} MAIN db: {e}")
#
#     logging.error(f"{serial_num}")
#     if th_data:
#         query = (
#             thermostat_table.select()
#             .join(zone_table)
#             .join(controller_table)
#             .join(building_table)
#             .join(zip_code_table)
#             .where(thermostat_table.c.serial_num == serial_num)
#             .with_only_columns(zip_code_table.c.todays_temp)
#         )
#
#         try:
#             with engine.connect() as conn:
#                 outdoor_temp = conn.execute(query).fetchone()[0]
#         except Exception as e:
#             outdoor_temp = None
#             logging.error(f"Unable to fetch temperature data from {serial_num} MAIN db: {e}")
#
#         try:
#             with db.connect() as conn:
#                 select_statement = (
#                     thermostat_temp_fluctuation.select()
#                     .where(thermostat_temp_fluctuation.c.sn == serial_num)
#                     .order_by(desc("timestamp"))
#                     .limit(THERMOSTAT_QUERY_LIMIT)
#                 )
#                 result_set = conn.execute(select_statement)
#                 data = result_set.fetchall()
#
#                 if data:
#                     if data[0][2] != int(th_data["t1"]):
#                         insert_statement = (
#                             thermostat_temp_fluctuation.insert().values(
#                                 sn=serial_num,
#                                 th_temp=int(th_data["t1"]),
#                                 outdoor_temp=outdoor_temp,
#                                 timestamp=now,
#                             )
#                         )
#                         conn.execute(insert_statement)
#                         conn.commit()
#                 else:
#                     insert_statement = (
#                         thermostat_temp_fluctuation.insert().values(
#                             sn=serial_num,
#                             th_temp=int(th_data["t1"]),
#                             outdoor_temp=outdoor_temp,
#                             timestamp=now,
#                         )
#                     )
#                     conn.execute(insert_statement)
#                     conn.commit()
#         except Exception as e:
#             logging.error(f"Unable to fetch sensor thermostat data from {serial_num} IoT db: {e}")
