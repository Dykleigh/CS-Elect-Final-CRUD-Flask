import os
import random
import sys
from datetime import date, timedelta

from flask import Flask
from flask_mysqldb import MySQL


def _make_app() -> Flask:
	app = Flask(__name__)
	app.config["MYSQL_USER"] = os.getenv("MYSQL_USER", "root")
	app.config["MYSQL_PASSWORD"] = os.getenv("MYSQL_PASSWORD", "password")
	app.config["MYSQL_HOST"] = os.getenv("MYSQL_HOST", "localhost")
	app.config["MYSQL_DB"] = os.getenv("MYSQL_DB", "hanz_sales_db")
	app.config["MYSQL_PORT"] = int(os.getenv("MYSQL_PORT", "3306"))
	app.config["MYSQL_CURSORCLASS"] = "DictCursor"
	return app


def ensure_min_sales(min_count: int = 20) -> int:
	app = _make_app()
	mysql = MySQL(app)

	with app.app_context():
		cur = mysql.connection.cursor()
		cur.execute("SELECT COUNT(*) AS c FROM sales_fact")
		current = int(cur.fetchone()["c"])
		if current >= min_count:
			return 0

		cur.execute("SELECT MAX(sale_id) AS m FROM sales_fact")
		max_id = cur.fetchone()["m"]
		next_id = int(max_id or 0) + 1

		cur.execute("SELECT product_id FROM products")
		product_ids = [r["product_id"] for r in cur.fetchall()]
		cur.execute("SELECT customer_id FROM customers")
		customer_ids = [r["customer_id"] for r in cur.fetchall()]
		cur.execute("SELECT region_id FROM regions")
		region_ids = [r["region_id"] for r in cur.fetchall()]

		if not product_ids or not customer_ids or not region_ids:
			raise RuntimeError("Missing dimension data (products/customers/regions). Load the schema seed first.")

		to_add = min_count - current
		base = date(2023, 1, 1)
		for _ in range(to_add):
			sale_id = next_id
			next_id += 1
			product_id = random.choice(product_ids)
			customer_id = random.choice(customer_ids)
			region_id = random.choice(region_ids)
			sale_date = base + timedelta(days=random.randint(0, 60))
			quantity = random.randint(1, 10)
			price = round(random.uniform(10.0, 1500.0), 2)

			cur.execute(
				"""
				INSERT INTO sales_fact (sale_id, product_id, sale_date, quantity, price, customer_id, region_id)
				VALUES (%s,%s,%s,%s,%s,%s,%s)
				""",
				(sale_id, product_id, sale_date, quantity, price, customer_id, region_id),
			)

		mysql.connection.commit()
		return to_add


if __name__ == "__main__":
	try:
		added = ensure_min_sales(20)
		print(f"Added {added} sales_fact rows")
	except Exception as exc:
		print(f"ERROR: {exc}")
		sys.exit(1)

