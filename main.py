import asyncio

from functions import (
    thermostat_data,
    controller_data, db,
)
from postgres import list_of_thermostats, list_of_controller, engine
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


async def run_parallel_async_functions(thermostats: list, controllers: list, conn_cold, conn_main):
    tasks = [
        asyncio.create_task(thermostats_all(thermostats, conn_cold, conn_main)),
        asyncio.create_task(controllers_all(controllers, conn_cold, conn_main)),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    with engine.connect() as conn_main:
        with db.connect() as conn_cold:
            thrms = list_of_thermostats(conn_main)
            ctrs = list_of_controller(conn_main)

            while True:
                asyncio.run(run_parallel_async_functions(thrms, ctrs, conn_cold, conn_main))

# def run():
#     with engine.connect() as conn_main:
#         with db.connect() as conn_cold:
#             list_of_thermostats()
#             for thr in thrs:
#                 thermostat_data(thr, conn_cold, conn_main)
#
#
# run()
