import asyncio

from functions import (
    thermostat_data,
    controller_data, db, switch_data, receiver_data,
)
from postgres import list_of_thermostats, list_of_controller, engine, list_of_receivers, list_of_switches
from redis_dir.daos import connect


async def thermostats_all(serial_nums, conn_cold, conn_main):
    tasks = []
    for serial_num in serial_nums:
        task = asyncio.create_task(thermostat_data(serial_num, conn_cold, conn_main, connect))
        tasks.append(task)

    await asyncio.gather(*tasks)


async def controllers_all(serial_nums, conn_cold, conn_main):
    tasks = []
    for serial_num in serial_nums:
        task = asyncio.create_task(controller_data(serial_num, conn_cold, conn_main, connect))
        tasks.append(task)

    await asyncio.gather(*tasks)


async def switches_all(serial_nums, conn_cold, conn_main):
    tasks = []
    for serial_num in serial_nums:
        task = asyncio.create_task(switch_data(serial_num, conn_cold, conn_main))
        tasks.append(task)

    await asyncio.gather(*tasks)


async def receivers_all(serial_nums, conn_cold):
    tasks = []
    for serial_num in serial_nums:
        task = asyncio.create_task(receiver_data(serial_num, conn_cold, connect))
        tasks.append(task)

    await asyncio.gather(*tasks)


async def run_parallel_async_functions(
        thermostats: list,
        controllers: list,
        switches: list,
        receivers: list,
        connect_cold,
        connect_main):
    tasks = [
        asyncio.create_task(thermostats_all(thermostats, connect_cold, connect_main)),
        asyncio.create_task(controllers_all(controllers, connect_cold, connect_main)),
        asyncio.create_task(switches_all(switches, connect_cold, connect_main)),
        asyncio.create_task(receivers_all(receivers, connect_cold)),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    with engine.connect() as connection_main:
        with db.connect() as connection_cold:
            thrms = list_of_thermostats(connection_main)
            ctrs = list_of_controller(connection_main)
            recs = list_of_receivers(connection_main)
            sws = list_of_switches(connection_main)

            while True:
                asyncio.run(run_parallel_async_functions(thrms, ctrs, sws, recs, connection_cold, connection_main))

# def run():
#     with engine.connect() as conn_main:
#         with db.connect() as conn_cold:
#             list_of_thermostats()
#             for thr in thrs:
#                 thermostat_data(thr, conn_cold, conn_main)
#
#
# run()
