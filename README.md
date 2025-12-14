## CS-Elect Final Project: CRUD Flask REST API (MySQL)

CRUD (Create/Read/Update/Delete) REST API built with Flask + MySQL, supporting JSON or XML output, JWT authentication, and search.

### Database schema
This project is designed for the `hanz_sales_db` schema (tables: `categories`, `regions`, `customers`, `products`, `sales_fact` and view `sales_denorm`).

Load your schema + seed data in MySQL using the SQL script provided in your course activity.

### Installation
1. Create and activate a virtual environment.
2. Install dependencies:

`pip install -r requirements.txt`

### Configuration (environment variables)
Set these (PowerShell example):

`$env:MYSQL_USER="root"`

`$env:MYSQL_PASSWORD="password"`

`$env:MYSQL_HOST="localhost"`

`$env:MYSQL_PORT="3306"`

`$env:MYSQL_DB="hanz_sales_db"`

Auth:

`$env:API_USERNAME="admin"`

`$env:API_PASSWORD="adminpassword"`

`$env:JWT_SECRET_KEY="your_jwt_secret_key"`

### Run the API
`python app.py`

Health check:

`GET http://127.0.0.1:5000/health`

### Authentication (JWT)
Login to get a token:

`POST /auth/login`

Body (JSON):

`{"username":"admin","password":"adminpassword"}`

Use the token in requests:

`Authorization: Bearer <token>`

### Output format (JSON or XML)
All endpoints accept a query parameter:

- JSON (default): `?format=json`
- XML: `?format=xml`

Example:

`GET /api/categories?format=xml`

### CRUD endpoints
All endpoints below require JWT.

Categories:

- `GET /api/categories`
- `POST /api/categories`
- `GET /api/categories/<category_id>`
- `PUT /api/categories/<category_id>`
- `DELETE /api/categories/<category_id>`

Regions:

- `GET /api/regions`
- `POST /api/regions`
- `GET /api/regions/<region_id>`
- `PUT /api/regions/<region_id>`
- `DELETE /api/regions/<region_id>`

Customers:

- `GET /api/customers`
- `POST /api/customers`
- `GET /api/customers/<customer_id>`
- `PUT /api/customers/<customer_id>`
- `DELETE /api/customers/<customer_id>`

Products:

- `GET /api/products`
- `POST /api/products`
- `GET /api/products/<product_id>`
- `PUT /api/products/<product_id>`
- `DELETE /api/products/<product_id>`

Sales (table: `sales_fact`):

- `GET /api/sales`
- `POST /api/sales`
- `GET /api/sales/<sale_id>`
- `PUT /api/sales/<sale_id>`
- `DELETE /api/sales/<sale_id>`

### Search
Search endpoint (uses view `sales_denorm`):

`GET /api/sales/search`

Supported query parameters:

- `product_name` (partial match)
- `category_name` (partial match)
- `region_name` (partial match)
- `customer_id` (exact match)
- `date_from` / `date_to` (YYYY-MM-DD)

Example:

`GET /api/sales/search?region_name=North&date_from=2023-01-01&date_to=2023-01-31`

### Tests
Tests are written with `unittest` and use Flask’s test client.

Before running tests:

1. Ensure MySQL is running.
2. Ensure the schema is loaded into `MYSQL_DB`.

Optional: ensure you have at least 20 records in `sales_fact`:

`python tests/insert_data.py`

Run tests:

`python -m unittest -v`