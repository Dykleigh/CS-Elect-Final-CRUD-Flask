from __future__ import annotations

import datetime as dt
import os
import re
from functools import wraps
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import jwt
import dicttoxml
from flask import Flask, Response, jsonify, make_response, request
from flask_mysqldb import MySQL
from werkzeug.exceptions import BadRequest, NotFound

from config import Config


mysql = MySQL()


def _parse_int(value: Any, field: str, *, minimum: Optional[int] = None) -> int:
	try:
		parsed = int(value)
	except (TypeError, ValueError):
		raise BadRequest(f"{field} must be an integer")
	if minimum is not None and parsed < minimum:
		raise BadRequest(f"{field} must be >= {minimum}")
	return parsed


def _parse_decimal(value: Any, field: str) -> float:
	try:
		parsed = float(value)
	except (TypeError, ValueError):
		raise BadRequest(f"{field} must be a number")
	return parsed


def _parse_date(value: Any, field: str) -> dt.date:
	if not value or not isinstance(value, str):
		raise BadRequest(f"{field} must be a date string (YYYY-MM-DD)")
	try:
		return dt.date.fromisoformat(value)
	except ValueError:
		raise BadRequest(f"{field} must be a date string (YYYY-MM-DD)")


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _validate_email(email: Any) -> str:
	if not isinstance(email, str) or not _EMAIL_RE.match(email):
		raise BadRequest("email must be a valid email address")
	return email


def _get_format() -> str:
	fmt = (request.args.get("format") or "json").strip().lower()
	if fmt not in {"json", "xml"}:
		raise BadRequest("format must be 'json' or 'xml'")
	return fmt


def _to_xml(payload: Any, root: str = "response") -> bytes:
	# dicttoxml wraps lists; make output predictable
    return dicttoxml.dicttoxml(payload, custom_root=root, attr_type=False)
def api_response(payload: Any, status: int = 200, *, root: str = "response") -> Response:
	fmt = _get_format()
	if fmt == "xml":
		xml_bytes = _to_xml(payload, root=root)
		resp = make_response(xml_bytes, status)
		resp.headers["Content-Type"] = "application/xml; charset=utf-8"
		return resp
	return make_response(jsonify(payload), status)


def error_response(message: str, status: int, *, details: Optional[Dict[str, Any]] = None) -> Response:
	payload: Dict[str, Any] = {"error": message, "status": status}
	if details:
		payload["details"] = details
	return api_response(payload, status=status, root="error")


def _generate_token(username: str, secret: str, *, expires_minutes: int = 60) -> str:
	now = dt.datetime.now(dt.timezone.utc)
	payload = {
		"sub": username,
		"iat": int(now.timestamp()),
		"exp": int((now + dt.timedelta(minutes=expires_minutes)).timestamp()),
	}
	return jwt.encode(payload, secret, algorithm="HS256")


def require_jwt(fn: Callable[..., Response]) -> Callable[..., Response]:
	@wraps(fn)
	def wrapper(*args: Any, **kwargs: Any) -> Response:
		header = request.headers.get("Authorization", "")
		if not header.startswith("Bearer "):
			return error_response("Missing or invalid Authorization header", 401)
		token = header.split(" ", 1)[1].strip()
		try:
			jwt.decode(token, current_app().config["JWT_SECRET_KEY"], algorithms=["HS256"])
		except jwt.ExpiredSignatureError:
			return error_response("Token expired", 401)
		except jwt.InvalidTokenError:
			return error_response("Invalid token", 401)
		return fn(*args, **kwargs)

	return wrapper


def current_app() -> Flask:
	# Local import avoids circulars if used early
	from flask import current_app as _ca

	return _ca  # type: ignore[return-value]


def _fetchone_dict(cursor) -> Optional[Dict[str, Any]]:
	row = cursor.fetchone()
	if row is None:
		return None
	if isinstance(row, dict):
		return row
	# MySQLdb typically returns tuples, but flask-mysqldb uses DictCursor only if set
	desc = [col[0] for col in cursor.description]
	return dict(zip(desc, row))


def _fetchall_dict(cursor) -> List[Dict[str, Any]]:
	rows = cursor.fetchall() or []
	if rows and isinstance(rows[0], dict):
		return list(rows)
	desc = [col[0] for col in cursor.description]
	return [dict(zip(desc, r)) for r in rows]


def _db() -> Tuple[Any, Any]:
	conn = mysql.connection
	cur = conn.cursor()
	return conn, cur


def _handle_db_error(exc: Exception) -> Response:
	msg = str(exc)
	# MySQL duplicate key
	if "Duplicate entry" in msg:
		return error_response("Conflict (duplicate key)", 409)
	return error_response("Database error", 500)


def create_app() -> Flask:
	app = Flask(__name__)
	app.config.from_object(Config)

	# Ensure env vars always take precedence (Config class attributes are evaluated at import time).
	def _env(name: str, default: Any) -> Any:
		value = os.getenv(name)
		if value is None:
			return default
		return value

	app.config["MYSQL_USER"] = _env("MYSQL_USER", app.config.get("MYSQL_USER"))
	app.config["MYSQL_PASSWORD"] = _env("MYSQL_PASSWORD", app.config.get("MYSQL_PASSWORD"))
	app.config["MYSQL_HOST"] = _env("MYSQL_HOST", app.config.get("MYSQL_HOST"))
	app.config["MYSQL_DB"] = _env("MYSQL_DB", app.config.get("MYSQL_DB"))
	app.config["MYSQL_PORT"] = int(_env("MYSQL_PORT", app.config.get("MYSQL_PORT", 3306)))
	app.config["MYSQL_POOL_NAME"] = _env("MYSQL_POOL_NAME", app.config.get("MYSQL_POOL_NAME", "mypool"))
	app.config["MYSQL_POOL_SIZE"] = int(_env("MYSQL_POOL_SIZE", app.config.get("MYSQL_POOL_SIZE", 5)))
	app.config["JWT_SECRET_KEY"] = _env("JWT_SECRET_KEY", app.config.get("JWT_SECRET_KEY"))
	app.config["API_USERNAME"] = _env("API_USERNAME", app.config.get("API_USERNAME"))
	app.config["API_PASSWORD"] = _env("API_PASSWORD", app.config.get("API_PASSWORD"))

	# flask-mysqldb expects these keys
	app.config.setdefault("MYSQL_USER", app.config.get("MYSQL_USER"))
	app.config.setdefault("MYSQL_PASSWORD", app.config.get("MYSQL_PASSWORD"))
	app.config.setdefault("MYSQL_HOST", app.config.get("MYSQL_HOST"))
	app.config.setdefault("MYSQL_DB", app.config.get("MYSQL_DB"))
	app.config.setdefault("MYSQL_PORT", app.config.get("MYSQL_PORT"))
	app.config.setdefault("MYSQL_CURSORCLASS", "DictCursor")

	mysql.init_app(app)

	@app.get("/health")
	def health() -> Response:
		return api_response({"status": "ok"})

	@app.post("/auth/login")
	def login() -> Response:
		body = request.get_json(silent=True) or {}
		username = body.get("username")
		password = body.get("password")
		if username != app.config.get("API_USERNAME") or password != app.config.get("API_PASSWORD"):
			return error_response("Invalid credentials", 401)
		token = _generate_token(str(username), app.config["JWT_SECRET_KEY"], expires_minutes=60)
		return api_response({"access_token": token, "token_type": "Bearer", "expires_in": 3600})

	# -------------------------
	# Categories CRUD
	# -------------------------
	@app.get("/api/categories")
	@require_jwt
	def list_categories() -> Response:
		try:
			_, cur = _db()
			cur.execute("SELECT category_id, category_name FROM categories ORDER BY category_id")
			return api_response({"items": _fetchall_dict(cur)})
		except Exception as e:
			return _handle_db_error(e)

	@app.post("/api/categories")
	@require_jwt
	def create_category() -> Response:
		body = request.get_json(silent=True) or {}
		name = (body.get("category_name") or "").strip()
		if not name:
			return error_response("category_name is required", 400)
		try:
			conn, cur = _db()
			cur.execute("INSERT INTO categories (category_name) VALUES (%s)", (name,))
			conn.commit()
			new_id = cur.lastrowid
			resp = api_response({"category_id": new_id, "category_name": name}, status=201)
			resp.headers["Location"] = f"/api/categories/{new_id}" + _format_suffix()
			return resp
		except Exception as e:
			return _handle_db_error(e)

	@app.get("/api/categories/<int:category_id>")
	@require_jwt
	def get_category(category_id: int) -> Response:
		try:
			_, cur = _db()
			cur.execute(
				"SELECT category_id, category_name FROM categories WHERE category_id=%s",
				(category_id,),
			)
			row = _fetchone_dict(cur)
			if not row:
				return error_response("Category not found", 404)
			return api_response(row)
		except Exception as e:
			return _handle_db_error(e)

	@app.put("/api/categories/<int:category_id>")
	@require_jwt
	def update_category(category_id: int) -> Response:
		body = request.get_json(silent=True) or {}
		name = (body.get("category_name") or "").strip()
		if not name:
			return error_response("category_name is required", 400)
		try:
			conn, cur = _db()
			cur.execute(
				"UPDATE categories SET category_name=%s WHERE category_id=%s",
				(name, category_id),
			)
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Category not found", 404)
			return api_response({"category_id": category_id, "category_name": name})
		except Exception as e:
			return _handle_db_error(e)

	@app.delete("/api/categories/<int:category_id>")
	@require_jwt
	def delete_category(category_id: int) -> Response:
		try:
			conn, cur = _db()
			cur.execute("DELETE FROM categories WHERE category_id=%s", (category_id,))
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Category not found", 404)
			return api_response({"deleted": True, "category_id": category_id})
		except Exception as e:
			return _handle_db_error(e)

	# -------------------------
	# Regions CRUD
	# -------------------------
	@app.get("/api/regions")
	@require_jwt
	def list_regions() -> Response:
		try:
			_, cur = _db()
			cur.execute("SELECT region_id, region_name FROM regions ORDER BY region_id")
			return api_response({"items": _fetchall_dict(cur)})
		except Exception as e:
			return _handle_db_error(e)

	@app.post("/api/regions")
	@require_jwt
	def create_region() -> Response:
		body = request.get_json(silent=True) or {}
		name = (body.get("region_name") or "").strip()
		if not name:
			return error_response("region_name is required", 400)
		try:
			conn, cur = _db()
			cur.execute("INSERT INTO regions (region_name) VALUES (%s)", (name,))
			conn.commit()
			new_id = cur.lastrowid
			resp = api_response({"region_id": new_id, "region_name": name}, status=201)
			resp.headers["Location"] = f"/api/regions/{new_id}" + _format_suffix()
			return resp
		except Exception as e:
			return _handle_db_error(e)

	@app.get("/api/regions/<int:region_id>")
	@require_jwt
	def get_region(region_id: int) -> Response:
		try:
			_, cur = _db()
			cur.execute("SELECT region_id, region_name FROM regions WHERE region_id=%s", (region_id,))
			row = _fetchone_dict(cur)
			if not row:
				return error_response("Region not found", 404)
			return api_response(row)
		except Exception as e:
			return _handle_db_error(e)

	@app.put("/api/regions/<int:region_id>")
	@require_jwt
	def update_region(region_id: int) -> Response:
		body = request.get_json(silent=True) or {}
		name = (body.get("region_name") or "").strip()
		if not name:
			return error_response("region_name is required", 400)
		try:
			conn, cur = _db()
			cur.execute("UPDATE regions SET region_name=%s WHERE region_id=%s", (name, region_id))
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Region not found", 404)
			return api_response({"region_id": region_id, "region_name": name})
		except Exception as e:
			return _handle_db_error(e)

	@app.delete("/api/regions/<int:region_id>")
	@require_jwt
	def delete_region(region_id: int) -> Response:
		try:
			conn, cur = _db()
			cur.execute("DELETE FROM regions WHERE region_id=%s", (region_id,))
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Region not found", 404)
			return api_response({"deleted": True, "region_id": region_id})
		except Exception as e:
			return _handle_db_error(e)

	# -------------------------
	# Customers CRUD
	# -------------------------
	@app.get("/api/customers")
	@require_jwt
	def list_customers() -> Response:
		try:
			_, cur = _db()
			cur.execute(
				"SELECT customer_id, first_name, last_name, email, signup_date FROM customers ORDER BY customer_id"
			)
			rows = _fetchall_dict(cur)
			for r in rows:
				if isinstance(r.get("signup_date"), (dt.date, dt.datetime)):
					r["signup_date"] = str(r["signup_date"])
			return api_response({"items": rows})
		except Exception as e:
			return _handle_db_error(e)

	@app.post("/api/customers")
	@require_jwt
	def create_customer() -> Response:
		body = request.get_json(silent=True) or {}
		customer_id = _parse_int(body.get("customer_id"), "customer_id", minimum=1)
		first_name = (body.get("first_name") or "").strip()
		last_name = (body.get("last_name") or "").strip()
		email = _validate_email(body.get("email"))
		signup_date = _parse_date(body.get("signup_date"), "signup_date")
		if not first_name or not last_name:
			return error_response("first_name and last_name are required", 400)
		try:
			conn, cur = _db()
			cur.execute(
				"INSERT INTO customers (customer_id, first_name, last_name, email, signup_date) VALUES (%s,%s,%s,%s,%s)",
				(customer_id, first_name, last_name, email, signup_date),
			)
			conn.commit()
			payload = {
				"customer_id": customer_id,
				"first_name": first_name,
				"last_name": last_name,
				"email": email,
				"signup_date": str(signup_date),
			}
			resp = api_response(payload, status=201)
			resp.headers["Location"] = f"/api/customers/{customer_id}" + _format_suffix()
			return resp
		except Exception as e:
			return _handle_db_error(e)

	@app.get("/api/customers/<int:customer_id>")
	@require_jwt
	def get_customer(customer_id: int) -> Response:
		try:
			_, cur = _db()
			cur.execute(
				"SELECT customer_id, first_name, last_name, email, signup_date FROM customers WHERE customer_id=%s",
				(customer_id,),
			)
			row = _fetchone_dict(cur)
			if not row:
				return error_response("Customer not found", 404)
			if isinstance(row.get("signup_date"), (dt.date, dt.datetime)):
				row["signup_date"] = str(row["signup_date"])
			return api_response(row)
		except Exception as e:
			return _handle_db_error(e)

	@app.put("/api/customers/<int:customer_id>")
	@require_jwt
	def update_customer(customer_id: int) -> Response:
		body = request.get_json(silent=True) or {}
		first_name = (body.get("first_name") or "").strip()
		last_name = (body.get("last_name") or "").strip()
		email = body.get("email")
		signup_date = body.get("signup_date")
		if not first_name or not last_name:
			return error_response("first_name and last_name are required", 400)
		if email is None or signup_date is None:
			return error_response("email and signup_date are required", 400)
		email = _validate_email(email)
		signup_date = _parse_date(signup_date, "signup_date")

		try:
			conn, cur = _db()
			cur.execute(
				"UPDATE customers SET first_name=%s, last_name=%s, email=%s, signup_date=%s WHERE customer_id=%s",
				(
					first_name,
					last_name,
					email,
					signup_date,
					customer_id,
				),
			)
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Customer not found", 404)
			return api_response(
				{
					"customer_id": customer_id,
					"first_name": first_name,
					"last_name": last_name,
					"email": email,
					"signup_date": str(signup_date),
				}
			)
		except Exception as e:
			return _handle_db_error(e)

	@app.delete("/api/customers/<int:customer_id>")
	@require_jwt
	def delete_customer(customer_id: int) -> Response:
		try:
			conn, cur = _db()
			cur.execute("DELETE FROM customers WHERE customer_id=%s", (customer_id,))
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Customer not found", 404)
			return api_response({"deleted": True, "customer_id": customer_id})
		except Exception as e:
			return _handle_db_error(e)

	# -------------------------
	# Products CRUD
	# -------------------------
	@app.get("/api/products")
	@require_jwt
	def list_products() -> Response:
		try:
			_, cur = _db()
			cur.execute(
				"""
				SELECT p.product_id, p.product_name, p.category_id, c.category_name
				FROM products p
				JOIN categories c ON c.category_id = p.category_id
				ORDER BY p.product_id
				"""
			)
			return api_response({"items": _fetchall_dict(cur)})
		except Exception as e:
			return _handle_db_error(e)

	@app.post("/api/products")
	@require_jwt
	def create_product() -> Response:
		body = request.get_json(silent=True) or {}
		name = (body.get("product_name") or "").strip()
		category_id = _parse_int(body.get("category_id"), "category_id", minimum=1)
		if not name:
			return error_response("product_name is required", 400)
		try:
			conn, cur = _db()
			cur.execute("INSERT INTO products (product_name, category_id) VALUES (%s,%s)", (name, category_id))
			conn.commit()
			new_id = cur.lastrowid
			resp = api_response({"product_id": new_id, "product_name": name, "category_id": category_id}, status=201)
			resp.headers["Location"] = f"/api/products/{new_id}" + _format_suffix()
			return resp
		except Exception as e:
			return _handle_db_error(e)

	@app.get("/api/products/<int:product_id>")
	@require_jwt
	def get_product(product_id: int) -> Response:
		try:
			_, cur = _db()
			cur.execute(
				"""
				SELECT p.product_id, p.product_name, p.category_id, c.category_name
				FROM products p
				JOIN categories c ON c.category_id = p.category_id
				WHERE p.product_id=%s
				""",
				(product_id,),
			)
			row = _fetchone_dict(cur)
			if not row:
				return error_response("Product not found", 404)
			return api_response(row)
		except Exception as e:
			return _handle_db_error(e)

	@app.put("/api/products/<int:product_id>")
	@require_jwt
	def update_product(product_id: int) -> Response:
		body = request.get_json(silent=True) or {}
		name = (body.get("product_name") or "").strip()
		category_id = _parse_int(body.get("category_id"), "category_id", minimum=1)
		if not name:
			return error_response("product_name is required", 400)
		try:
			conn, cur = _db()
			cur.execute(
				"UPDATE products SET product_name=%s, category_id=%s WHERE product_id=%s",
				(name, category_id, product_id),
			)
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Product not found", 404)
			return api_response({"product_id": product_id, "product_name": name, "category_id": category_id})
		except Exception as e:
			return _handle_db_error(e)

	@app.delete("/api/products/<int:product_id>")
	@require_jwt
	def delete_product(product_id: int) -> Response:
		try:
			conn, cur = _db()
			cur.execute("DELETE FROM products WHERE product_id=%s", (product_id,))
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Product not found", 404)
			return api_response({"deleted": True, "product_id": product_id})
		except Exception as e:
			return _handle_db_error(e)

	# -------------------------
	# Sales CRUD (sales_fact)
	# -------------------------
	@app.get("/api/sales")
	@require_jwt
	def list_sales() -> Response:
		try:
			_, cur = _db()
			cur.execute(
				"""
				SELECT sale_id, product_id, sale_date, quantity, price, customer_id, region_id
				FROM sales_fact
				ORDER BY sale_id
				"""
			)
			rows = _fetchall_dict(cur)
			for r in rows:
				if isinstance(r.get("sale_date"), (dt.date, dt.datetime)):
					r["sale_date"] = str(r["sale_date"])
			return api_response({"items": rows})
		except Exception as e:
			return _handle_db_error(e)

	@app.post("/api/sales")
	@require_jwt
	def create_sale() -> Response:
		body = request.get_json(silent=True) or {}
		sale_id = _parse_int(body.get("sale_id"), "sale_id", minimum=1)
		product_id = _parse_int(body.get("product_id"), "product_id", minimum=1)
		sale_date = _parse_date(body.get("sale_date"), "sale_date")
		quantity = _parse_int(body.get("quantity"), "quantity", minimum=1)
		price = _parse_decimal(body.get("price"), "price")
		customer_id = _parse_int(body.get("customer_id"), "customer_id", minimum=1)
		region_id = _parse_int(body.get("region_id"), "region_id", minimum=1)

		try:
			conn, cur = _db()
			cur.execute(
				"""
				INSERT INTO sales_fact (sale_id, product_id, sale_date, quantity, price, customer_id, region_id)
				VALUES (%s,%s,%s,%s,%s,%s,%s)
				""",
				(sale_id, product_id, sale_date, quantity, price, customer_id, region_id),
			)
			conn.commit()
			payload = {
				"sale_id": sale_id,
				"product_id": product_id,
				"sale_date": str(sale_date),
				"quantity": quantity,
				"price": float(price),
				"customer_id": customer_id,
				"region_id": region_id,
			}
			resp = api_response(payload, status=201)
			resp.headers["Location"] = f"/api/sales/{sale_id}" + _format_suffix()
			return resp
		except Exception as e:
			return _handle_db_error(e)

	@app.get("/api/sales/<int:sale_id>")
	@require_jwt
	def get_sale(sale_id: int) -> Response:
		try:
			_, cur = _db()
			cur.execute(
				"SELECT sale_id, product_id, sale_date, quantity, price, customer_id, region_id FROM sales_fact WHERE sale_id=%s",
				(sale_id,),
			)
			row = _fetchone_dict(cur)
			if not row:
				return error_response("Sale not found", 404)
			if isinstance(row.get("sale_date"), (dt.date, dt.datetime)):
				row["sale_date"] = str(row["sale_date"])
			return api_response(row)
		except Exception as e:
			return _handle_db_error(e)

	@app.put("/api/sales/<int:sale_id>")
	@require_jwt
	def update_sale(sale_id: int) -> Response:
		body = request.get_json(silent=True) or {}
		product_id = _parse_int(body.get("product_id"), "product_id", minimum=1)
		sale_date = _parse_date(body.get("sale_date"), "sale_date")
		quantity = _parse_int(body.get("quantity"), "quantity", minimum=1)
		price = _parse_decimal(body.get("price"), "price")
		customer_id = _parse_int(body.get("customer_id"), "customer_id", minimum=1)
		region_id = _parse_int(body.get("region_id"), "region_id", minimum=1)
		try:
			conn, cur = _db()
			cur.execute(
				"""
				UPDATE sales_fact
				SET product_id=%s, sale_date=%s, quantity=%s, price=%s, customer_id=%s, region_id=%s
				WHERE sale_id=%s
				""",
				(product_id, sale_date, quantity, price, customer_id, region_id, sale_id),
			)
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Sale not found", 404)
			return api_response(
				{
					"sale_id": sale_id,
					"product_id": product_id,
					"sale_date": str(sale_date),
					"quantity": quantity,
					"price": float(price),
					"customer_id": customer_id,
					"region_id": region_id,
				}
			)
		except Exception as e:
			return _handle_db_error(e)

	@app.delete("/api/sales/<int:sale_id>")
	@require_jwt
	def delete_sale(sale_id: int) -> Response:
		try:
			conn, cur = _db()
			cur.execute("DELETE FROM sales_fact WHERE sale_id=%s", (sale_id,))
			conn.commit()
			if cur.rowcount == 0:
				return error_response("Sale not found", 404)
			return api_response({"deleted": True, "sale_id": sale_id})
		except Exception as e:
			return _handle_db_error(e)

	# -------------------------
	# Search (uses view)
	# -------------------------
	@app.get("/api/sales/search")
	@require_jwt
	def search_sales() -> Response:
		product_name = request.args.get("product_name")
		category_name = request.args.get("category_name")
		region_name = request.args.get("region_name")
		customer_id = request.args.get("customer_id")
		date_from = request.args.get("date_from")
		date_to = request.args.get("date_to")

		where: List[str] = []
		params: List[Any] = []

		if product_name:
			where.append("product_name LIKE %s")
			params.append(f"%{product_name}%")
		if category_name:
			where.append("product_category LIKE %s")
			params.append(f"%{category_name}%")
		if region_name:
			where.append("region LIKE %s")
			params.append(f"%{region_name}%")
		if customer_id:
			where.append("customer_id = %s")
			params.append(_parse_int(customer_id, "customer_id", minimum=1))
		if date_from:
			where.append("sale_date >= %s")
			params.append(_parse_date(date_from, "date_from"))
		if date_to:
			where.append("sale_date <= %s")
			params.append(_parse_date(date_to, "date_to"))

		sql = "SELECT * FROM sales_denorm"
		if where:
			sql += " WHERE " + " AND ".join(where)
		sql += " ORDER BY sale_id"

		try:
			_, cur = _db()
			cur.execute(sql, tuple(params))
			rows = _fetchall_dict(cur)
			for r in rows:
				if isinstance(r.get("sale_date"), (dt.date, dt.datetime)):
					r["sale_date"] = str(r["sale_date"])
				if isinstance(r.get("signup_date"), (dt.date, dt.datetime)):
					r["signup_date"] = str(r["signup_date"])
			return api_response({"items": rows, "count": len(rows)})
		except Exception as e:
			return _handle_db_error(e)

	# -------------------------
	# Consistent JSON/XML errors
	# -------------------------
	@app.errorhandler(BadRequest)
	def _bad_request(err: BadRequest):
		return error_response(str(err.description or "Bad request"), 400)

	@app.errorhandler(NotFound)
	def _not_found(err: NotFound):
		return error_response("Not found", 404)

	@app.errorhandler(Exception)
	def _unhandled(err: Exception):
		# In production you would log err.
		return error_response("Internal server error", 500)

	return app


def _format_suffix() -> str:
	fmt = request.args.get("format")
	if fmt:
		return f"?format={fmt}"
	return ""


app = create_app()


if __name__ == "__main__":
	port = int(os.getenv("PORT", 5000))
	app.run(host="0.0.0.0", port=port, debug=True)
