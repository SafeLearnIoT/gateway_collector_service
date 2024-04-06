import pandas as pd
import aiomqtt
import asyncio
import os
from sqlalchemy import create_engine
from datetime import datetime
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

aiomqtt.Client(
    hostname=os.environ.get("MQTT_HOST"), port=int(os.environ.get("MQTT_PORT"))
)

engine = create_engine(
    f"mysql+mysqlconnector://{os.environ.get('MYSQL_USER')}:{os.environ.get('MYSQL_PASS')}@{os.environ.get('MYSQL_HOST')}:{os.environ.get('MYSQL_PORT')}/{os.environ.get('MYSQL_DB')}"
)


async def main():
    async with aiomqtt.Client("192.168.0.164") as client:

        await client.subscribe("esp8266/inside/data")
        await client.subscribe("esp8266/outside/data")
        await client.subscribe("esp32/light/data")
        await client.subscribe("esp32/window/data")

        async for message in client.messages:
            df = pd.read_csv(StringIO(message.payload.decode()))
            df["timestamp"] = df["timestamp"].apply(lambda x: datetime.fromtimestamp(x))
            if message.topic.matches("esp32/light/data"):
                df.to_sql(
                    "ambient_light_data", con=engine, index=False, if_exists="append"
                )
            if message.topic.matches("esp32/window/data"):
                df.to_sql(
                    "window_status_data", con=engine, index=False, if_exists="append"
                )
            if message.topic.matches("esp8266/outside/data"):
                df.to_sql(
                    "outside_env_data", con=engine, index=False, if_exists="append"
                )
            if message.topic.matches("esp8266/inside/data"):
                df.to_sql(
                    "inside_env_data", con=engine, index=False, if_exists="append"
                )


asyncio.run(main())
