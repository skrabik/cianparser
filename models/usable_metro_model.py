from sqlalchemy import table, column

usable_metro = table("usable_metro",
        column('metro_id'),
         column('metro_name')
)