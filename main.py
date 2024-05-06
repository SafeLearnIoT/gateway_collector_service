import pandas as pd
import aiomqtt
import asyncio
import os
import sqlalchemy
from datetime import datetime
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

aiomqtt.Client(
    hostname=os.environ.get("MQTT_HOST"), port=int(os.environ.get("MQTT_PORT"))
)

engine = sqlalchemy.create_engine(
    f"mysql+mysqlconnector://{os.environ.get('MYSQL_USER')}:{os.environ.get('MYSQL_PASS')}@{os.environ.get('MYSQL_HOST')}:{os.environ.get('MYSQL_PORT')}/{os.environ.get('MYSQL_DB')}"
)


async def commit_activity(message, device):
    with engine.connect() as conn:
        data = message.split(";")
        conn.execute(
            sqlalchemy.text(
                f"INSERT INTO activity (timestamp, device, action, runtime) VALUE ('{datetime.now()}', '{device}', '{data[0]}', {data[1]})"
            )
        )
        conn.commit()


async def main():
    async with aiomqtt.Client("192.168.0.164") as client:

        await client.subscribe("esp8266/inside/data")
        await client.subscribe("esp8266/outside/data")
        await client.subscribe("esp32/light/data")
        await client.subscribe("esp32/window/data")

        await client.subscribe("esp8266/inside/status")
        await client.subscribe("esp8266/outside/status")
        await client.subscribe("esp32/light/status")
        await client.subscribe("esp32/window/status")

        async for message in client.messages:
            if message.topic.matches("esp32/light/data"):
                df = pd.read_csv(StringIO(message.payload.decode()))
                df["timestamp"] = df["timestamp"].apply(
                    lambda x: datetime.fromtimestamp(x)
                )
                df.to_sql(
                    "ambient_light_data", con=engine, index=False, if_exists="append"
                )
            if message.topic.matches("esp32/window/data"):
                df = pd.read_csv(StringIO(message.payload.decode()))
                df["timestamp"] = df["timestamp"].apply(
                    lambda x: datetime.fromtimestamp(x)
                )
                df.to_sql(
                    "window_status_data", con=engine, index=False, if_exists="append"
                )
            if message.topic.matches("esp8266/outside/data"):
                df = pd.read_csv(StringIO(message.payload.decode()))
                df["timestamp"] = df["timestamp"].apply(
                    lambda x: datetime.fromtimestamp(x)
                )
                df.to_sql(
                    "outside_env_data", con=engine, index=False, if_exists="append"
                )
            if message.topic.matches("esp8266/inside/data"):
                df = pd.read_csv(StringIO(message.payload.decode()))
                df["timestamp"] = df["timestamp"].apply(
                    lambda x: datetime.fromtimestamp(x)
                )
                df.to_sql(
                    "inside_env_data", con=engine, index=False, if_exists="append"
                )

            if message.topic.matches("esp8266/inside/status"):
                await commit_activity(message.payload.decode(), "esp8266/inside")

            if message.topic.matches("esp8266/outside/status"):
                await commit_activity(message.payload.decode(), "esp8266/outside")

            if message.topic.matches("esp32/light/status"):
                await commit_activity(message.payload.decode(), "esp32/light")

            if message.topic.matches("esp32/window/status"):
                await commit_activity(message.payload.decode(), "esp32/window")


asyncio.run(main())
