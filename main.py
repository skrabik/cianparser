import sqlalchemy
from sqlalchemy import table, column, insert, select

import cianparser
from db_config import *

flat = table("flat",
        column("id"),
        column("cian_id"),
        column("url"),
        column("total_meters"),
        column("price"),
        column("commissions"),
        column("district"),
        column("underground"),
        column("author_type"),
)

usable_metro = table("usable_metro",
        column('metro_id'),
         column('metro_name')
)

usable_districts = table("usable_districts",
        column('district_id'),
         column('district_name')
)

engine = sqlalchemy.create_engine("postgresql://"+DB_HOST+":"+DB_PORT+"/"+DB_NAME+"?password="+DB_PASSWORD+"&user="+DB_USER)

with engine.connect() as conn:
    stmt = select(usable_metro)
    metro_list = [el[1] for el in conn.execute(stmt)]

with engine.connect() as conn:
    stmt = select(usable_districts)
    districts_list = [el[1] for el in conn.execute(stmt)]

parser = cianparser.CianParser(location="Москва")

for station in metro_list:
    additional_settings = {
        "max_price": 65000,
        # 'metro': 'Московский',
        # "metro_station": station,
        'district': 103,
        "start_page": 1,
        "end_page": 5
    }
    data = parser.get_flats(deal_type="rent_long", rooms=(1, 2), with_saving_csv=False, additional_settings=additional_settings)
    with engine.connect() as conn:
        for el in data:
            stmt = insert(flat).values(
                cian_id=el['url'].split('/')[-2],
                url=el['url'],
                total_meters=el['total_meters'],
                price=el['price_per_month'],
                commissions=el['commissions'],
                district=el['district'],
                underground=el['underground']
            )
            conn.execute(stmt)
            conn.commit()