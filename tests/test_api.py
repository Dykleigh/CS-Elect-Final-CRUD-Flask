import os
import time
import unittest


class ApiTests(unittest.TestCase):
	@classmethod
	def setUpClass(cls) -> None:
		# Allow users to override these in their shell.
		os.environ.setdefault("MYSQL_DB", "hanz_sales_db")
		os.environ.setdefault("API_USERNAME", "admin")
		os.environ.setdefault("API_PASSWORD", "adminpassword")
		os.environ.setdefault("JWT_SECRET_KEY", "your_jwt_secret_key")

		from app import create_app  # local import so env vars above are applied

		cls.app = create_app()
		cls.app.config["TESTING"] = True
		cls.client = cls.app.test_client()

		cls.token = cls._login_and_get_token()
		cls.headers = {"Authorization": f"Bearer {cls.token}"}

		# Verify DB is reachable and required tables exist; otherwise skip.
		try:
			with cls.app.app_context():
				from app import mysql

				cur = mysql.connection.cursor()
				cur.execute("SELECT 1")
				cur.execute("SHOW TABLES")
				tables = {list(r.values())[0] if isinstance(r, dict) else r[0] for r in cur.fetchall()}
				required = {"categories", "regions", "customers", "products", "sales_fact"}
				if not required.issubset(tables):
					missing = ",".join(sorted(required - tables))
					raise RuntimeError(f"Missing tables: {missing}")
		except Exception as exc:
			raise unittest.SkipTest(
				"MySQL not reachable or schema not loaded. Run your SQL schema and set env vars (MYSQL_*) first. "
				f"Details: {exc}"
			)

	@classmethod
	def _login_and_get_token(cls) -> str:
		resp = cls.client.post(
			"/auth/login",
			json={"username": os.environ.get("API_USERNAME"), "password": os.environ.get("API_PASSWORD")},
		)
		if resp.status_code != 200:
			raise AssertionError(f"Login failed: {resp.status_code} {resp.data!r}")
		data = resp.get_json()
		if not data or "access_token" not in data:
			raise AssertionError(f"Login response missing token: {data!r}")
		return data["access_token"]

	def test_requires_auth(self):
		resp = self.client.get("/api/categories")
		self.assertEqual(resp.status_code, 401)

	def test_xml_formatting(self):
		resp = self.client.get("/api/categories?format=xml", headers=self.headers)
		self.assertEqual(resp.status_code, 200)
		self.assertIn("application/xml", resp.headers.get("Content-Type", ""))

	def test_customer_validation(self):
		payload = {
			"customer_id": 999001,
			"first_name": "Test",
			"last_name": "User",
			"email": "not-an-email",
			"signup_date": "2023-01-01",
		}
		resp = self.client.post("/api/customers", json=payload, headers=self.headers)
		self.assertEqual(resp.status_code, 400)

	def test_full_crud_flow(self):
		suffix = str(int(time.time()))
		created = {}

		# Create category
		r = self.client.post(
			"/api/categories",
			json={"category_name": f"TestCategory{suffix}"},
			headers=self.headers,
		)
		self.assertEqual(r.status_code, 201)
		created["category"] = r.get_json()
		category_id = created["category"]["category_id"]

		# Create region
		r = self.client.post(
			"/api/regions",
			json={"region_name": f"TestRegion{suffix}"},
			headers=self.headers,
		)
		self.assertEqual(r.status_code, 201)
		created["region"] = r.get_json()
		region_id = created["region"]["region_id"]

		# Create customer (choose a high ID to avoid collisions)
		customer_id = int(f"9{suffix[-5:]}01")
		r = self.client.post(
			"/api/customers",
			json={
				"customer_id": customer_id,
				"first_name": "Test",
				"last_name": "User",
				"email": f"test.user.{suffix}@example.com",
				"signup_date": "2023-01-01",
			},
			headers=self.headers,
		)
		# If collision happens, treat as non-fatal for local DBs.
		if r.status_code == 409:
			raise unittest.SkipTest("Customer ID/email collided with existing data. Re-run test.")
		self.assertEqual(r.status_code, 201)
		created["customer"] = r.get_json()

		# Create product
		r = self.client.post(
			"/api/products",
			json={"product_name": f"TestProduct{suffix}", "category_id": category_id},
			headers=self.headers,
		)
		self.assertEqual(r.status_code, 201)
		created["product"] = r.get_json()
		product_id = created["product"]["product_id"]

		# Create sale
		sale_id = int(f"8{suffix[-6:]}1")
		r = self.client.post(
			"/api/sales",
			json={
				"sale_id": sale_id,
				"product_id": product_id,
				"sale_date": "2023-02-01",
				"quantity": 2,
				"price": 19.99,
				"customer_id": customer_id,
				"region_id": region_id,
			},
			headers=self.headers,
		)
		if r.status_code == 409:
			raise unittest.SkipTest("Sale ID collided with existing data. Re-run test.")
		self.assertEqual(r.status_code, 201)
		created["sale"] = r.get_json()

		# Read sale
		r = self.client.get(f"/api/sales/{sale_id}", headers=self.headers)
		self.assertEqual(r.status_code, 200)

		# Search should find it
		r = self.client.get(f"/api/sales/search?customer_id={customer_id}", headers=self.headers)
		self.assertEqual(r.status_code, 200)
		data = r.get_json()
		self.assertGreaterEqual(data.get("count", 0), 1)

		# Update sale
		r = self.client.put(
			f"/api/sales/{sale_id}",
			json={
				"product_id": product_id,
				"sale_date": "2023-02-02",
				"quantity": 3,
				"price": 29.99,
				"customer_id": customer_id,
				"region_id": region_id,
			},
			headers=self.headers,
		)
		self.assertEqual(r.status_code, 200)

		# Cleanup: delete sale -> product -> customer -> category/region
		self.assertEqual(self.client.delete(f"/api/sales/{sale_id}", headers=self.headers).status_code, 200)
		self.assertEqual(self.client.delete(f"/api/products/{product_id}", headers=self.headers).status_code, 200)
		self.assertEqual(self.client.delete(f"/api/customers/{customer_id}", headers=self.headers).status_code, 200)
		self.assertEqual(self.client.delete(f"/api/categories/{category_id}", headers=self.headers).status_code, 200)
		self.assertEqual(self.client.delete(f"/api/regions/{region_id}", headers=self.headers).status_code, 200)


if __name__ == "__main__":
	unittest.main()

