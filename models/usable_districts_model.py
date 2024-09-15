from sqlalchemy import table, column

usable_districts = table("usable_districts",
        column('district_id'),
         column('district_name')
)