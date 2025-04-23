import random
import aiomqtt
import asyncio
import os
import ast
import pymysql
from datetime import datetime
import json
from dotenv import load_dotenv
from enum import IntEnum

class TYPE(IntEnum):
    FLOOD = 1
    HOLD = 2

ATTACK_TYPE = TYPE.FLOOD

DEVICES = ["esp8266/inside", "esp8266/outside", "esp32/light", "esp32/window"]

load_dotenv()

# DATA
data_query = """
insert into safelearniot.data (device, data, time_sent, time_saved)
values (%s, %s, %s, %s);
"""
# ML
ml_query = """
insert into safelearniot.ml (device, data, time_sent, time_saved)
values (%s, %s, %s, %s);
"""

# PARAMS
def params_query(device):
    return f"""
select * from safelearniot.ml where device = '{device}' and JSON_VALUE(data, '$.type') = 'params' order by time_saved desc limit 1;
"""

# STATUS
status_query = """
insert into safelearniot.status (active_time, device, status, time_sent, time_saved, test_name)
values (%s, %s, %s, %s, %s, %s);
"""


async def send_weights(client: aiomqtt.Client):
    pass
    # while True:
    #     for device in DEVICES:
    #         await client.subscribe(f"cmd_mcu/{device}")
    #         await client.publish(f"cmd_gateway/{device}", "get_params", retain=True)
    #         print(f"Published 'get_params' on cmd_gateway/{device}")
    #     await asyncio.sleep(4 * 60 * 60)


async def main():
    connection = pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASS"],
        database=os.environ["MYSQL_DB"],
        port=int(os.environ["MYSQL_PORT"]),
    )
    cursor = connection.cursor()

    async with aiomqtt.Client(hostname=os.environ.get("MQTT_HOST"), port=int(os.environ.get("MQTT_PORT"))) as client:
        asyncio.create_task(send_weights(client))

        for device in DEVICES:
            await client.subscribe(f"data/{device}")
            await client.subscribe(f"ml/{device}")
            await client.subscribe(f"status/{device}")
            await client.subscribe(f"cmd_mcu/{device}")

        async for message in client.messages:
            if message.topic.matches("cmd_mcu/#"):
                msg = message.payload.decode()
                if msg != "":
                    if msg != "get_params":
                        data = ast.literal_eval(message.payload.decode()[7:])

                        data["time_sent"] = datetime.fromtimestamp(data["time_sent"])
                        data["time_saved"] = datetime.now()
                        values = (
                            data["device"],
                            json.dumps(data["data"]),
                            data["time_sent"],
                            data["time_saved"],
                        )
                        cursor.execute(ml_query, values)
                        connection.commit()
                        await asyncio.sleep(1)
                        await client.publish(message.topic.value.replace('cmd_mcu', 'cmd_gateway'), f"set_params;{json.dumps(data['data'])}".encode(), retain=True)
                        await client.publish(message.topic.value, "", retain=True)
                        print(f"Published resp 'set_params' on {message.topic.value.replace('cmd_mcu', 'cmd_gateway')}")
                    else:
                        cursor.execute(params_query(message.topic.value[8:]))
                        result = cursor.fetchone()
                        data_str = f"set_params;{result[2]}"
                        #print(data_str)
                        await client.publish(message.topic.value.replace('cmd_mcu', 'cmd_gateway'), data_str.encode(), retain=True)
                        await client.publish(message.topic.value, "", retain=True)
                        print(f"Published 'set_params' on {message.topic.value.replace('cmd_mcu', 'cmd_gateway')}")
                

            if message.topic.matches("data/#"):
                data = ast.literal_eval(message.payload.decode())
                data["time_sent"] = datetime.fromtimestamp(data["time_sent"])
                data["time_saved"] = datetime.now()
                values = (
                    data["device"],
                    json.dumps(data["data"]),
                    data["time_sent"],
                    data["time_saved"],
                )
                cursor.execute(data_query, values)
                connection.commit()

            if message.topic.matches("ml/#"):
                data = ast.literal_eval(message.payload.decode())
                data["time_sent"] = datetime.fromtimestamp(data["time_sent"])
                data["time_saved"] = datetime.now()
                values = (
                    data["device"],
                    json.dumps(data["data"]),
                    data["time_sent"],
                    data["time_saved"],
                )
                cursor.execute(ml_query, values)
                connection.commit()

            if message.topic.matches("status/#"):
                data = ast.literal_eval(message.payload.decode())
                data["time_sent"] = datetime.fromtimestamp(data["time_sent"])
                data["time_saved"] = datetime.now()
                values = (
                    data["active_time"],
                    data["device"],
                    data["status"],
                    data["time_sent"],
                    data["time_saved"],
                    data["test_name"]
                )
                print(values)
                cursor.execute(status_query, values)
                connection.commit()
            


asyncio.run(main())
