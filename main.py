import sqlalchemy
from sqlalchemy import insert, select

from telegram_api.message import *
from telegram_api.allowed_chats import *
import cianparser
from models import flat, usable_metro, usable_districts
from config import *
from helpers import *

connection_string = "postgresql://"+DB_HOST+":"+DB_PORT+"/"+DB_NAME+"?password="+DB_PASSWORD+"&user="+DB_USER
engine = sqlalchemy.create_engine(connection_string,
                                      pool_size=10,
                                      max_overflow=2,
                                      pool_recycle=300,
                                      pool_pre_ping=True,
                                      pool_use_lifo=True)

with engine.connect() as conn:
    stmt = select(flat.c.cian_id)
    parsed_flats = {el[0] for el in conn.execute(stmt)}

    stmt = select(usable_metro)
    metro_list = [el[1] for el in conn.execute(stmt)]

    stmt = select(usable_districts)
    districts_list = [el[1] for el in conn.execute(stmt)]


max_price = 65000
parser = cianparser.CianParser(location="Москва")

for station in metro_list:
    additional_settings = {
        "max_price": max_price,
        'metro': 'Московский',
        "metro_station": station,
        "start_page": 1,
        "end_page": 1
    }
    data = parser.get_flats(deal_type="rent_long", rooms=(1, 2), with_saving_csv=False, additional_settings=additional_settings)
    with engine.connect() as conn:
        for el in data:
            cian_id = el['url'].split('/')[-2]
            if cian_id not in parsed_flats:
                parsed_flats.add(cian_id)
                conn.execute(insert_stmt(el))
                conn.commit()
                for user_id in allowed_chats:
                    message = build_message(el)
                    StaticMethods.sendText(BOT_TOKEN, user_id, message)



# for district in districts_list:
#     additional_settings = {
#         "max_price": max_price,
#         'city': 'Москва',
#         'district': district,
#         "start_page": 1,
#         "end_page": 5
#     }
#     data = parser.get_flats(deal_type="rent_long", rooms=(1, 2), with_saving_csv=False, additional_settings=additional_settings)
#     with engine.connect() as conn:
#         for el in data:
#             cian_id = el['url'].split('/')[-2]
#             if cian_id not in parsed_flats:
#                 try:
#                     conn.execute(insert_stmt(el))
#                     conn.commit()
#                     for user_id in allowed_chats:
#                         message = build_message(el)
#                         StaticMethods.sendText(BOT_TOKEN, user_id, message)
#                 except Exception as e:
#                     print(e)



