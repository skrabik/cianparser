from sqlalchemy import table, column

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