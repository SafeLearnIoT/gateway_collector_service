import aiomqtt
import asyncio
import os
import ast
import pymysql
from datetime import datetime
import json
from dotenv import load_dotenv

DEVICES = ["esp8266/inside", "esp8266/outside", "esp32/light", "esp32/window"]

load_dotenv()

aiomqtt.Client(
    hostname=os.environ.get("MQTT_HOST"), port=int(os.environ.get("MQTT_PORT"))
)

# DATA
data_query = """
insert into safelearniot.data (device, data, time_sent, time_saved)
values (%s, %s, %s, %s);
"""
# ML
ml_query = """
insert into ml (device, data, time_sent, time_saved)
values (%s, %s, %s, %s);
"""

# STATUS
status_query = """
insert into safelearniot.status (active_time, device, status, time_sent, time_saved)
values (%s, %s, %s, %s, %s);
"""


async def send_weights(client: aiomqtt.Client):
    while True:
        for device in DEVICES:
            await client.publish(f"cmd/{device}", "weights", retain=True)
            print(f"Published 'weights' on cmd/{device}")

        await asyncio.sleep(4 * 60 * 60)


async def main():
    connection = pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASS"],
        database=os.environ["MYSQL_DB"],
        port=int(os.environ["MYSQL_PORT"]),
    )
    cursor = connection.cursor()

    async with aiomqtt.Client("192.168.0.164") as client:
        asyncio.create_task(send_weights(client))

        for device in DEVICES:
            await client.subscribe(f"data/{device}")
            await client.subscribe(f"ml/{device}")
            await client.subscribe(f"status/{device}")

        async for message in client.messages:
            data = ast.literal_eval(message.payload.decode())
            data["time_sent"] = datetime.fromtimestamp(data["time_sent"])
            data["time_saved"] = datetime.now()

            if message.topic.matches("data/#"):
                values = (
                    data["device"],
                    json.dumps(data["data"]),
                    data["time_sent"],
                    data["time_saved"],
                )
                cursor.execute(data_query, values)
                connection.commit()

            if message.topic.matches("ml/#"):
                values = (
                    data["device"],
                    json.dumps(data["data"]),
                    data["time_sent"],
                    data["time_saved"],
                )
                cursor.execute(ml_query, values)
                connection.commit()

            if message.topic.matches("status/#"):
                values = (
                    data["active_time"],
                    data["device"],
                    data["status"],
                    data["time_sent"],
                    data["time_saved"],
                )
                cursor.execute(status_query, values)
                connection.commit()


asyncio.run(main())
