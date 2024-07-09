import aiomqtt
import asyncio
import os
import ast
import pymongo
from datetime import datetime
from dotenv import load_dotenv

DEVICES = ["esp8266/inside", "esp8266/outside", "esp32/light", "esp32/window"]

load_dotenv()

mongo_client = pymongo.MongoClient(
    f'mongodb://{os.environ.get("MONGO_USER")}:{os.environ.get("MONGO_PASS")}@{os.environ.get("MONGO_HOST")}:{os.environ.get("MONGO_PORT")}/'
)
mongo_db = mongo_client[os.environ.get("MONGO_DB")]
mongo_data_col = mongo_db[os.environ.get("MONGO_DATA_COL")]
mongo_status_col = mongo_db[os.environ.get("MONGO_STATUS_COL")]
mongo_ml_col = mongo_db[os.environ.get("MONGO_ML_COL")]

aiomqtt.Client(
    hostname=os.environ.get("MQTT_HOST"), port=int(os.environ.get("MQTT_PORT"))
)


# datetime.fromtimestamp(x)
async def main():
    async with aiomqtt.Client("192.168.0.164") as client:

        for device in DEVICES:
            await client.subscribe(f"data/{device}")
            await client.subscribe(f"ml/{device}")
            await client.subscribe(f"status/{device}")

        async for message in client.messages:
            data = ast.literal_eval(message.payload.decode())
            data["time"] = datetime.fromtimestamp(data["time"])

            if message.topic.matches("data/#"):
                mongo_data_col.insert_one(data)

            if message.topic.matches("ml/#"):
                mongo_ml_col.insert_one(data)

            if message.topic.matches("status/#"):
                mongo_status_col.insert_one(data)


asyncio.run(main())
