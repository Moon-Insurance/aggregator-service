import os
from urllib.parse import urlparse
from dotenv import load_dotenv
from flask import Flask
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from flask_sqlalchemy import SQLAlchemy

# Load environment variables from a .env file (if using python-dotenv)
load_dotenv()

app = Flask(__name__)

postgres_url_env = os.getenv("DATABASE_URL")
parsed_postgres = urlparse(postgres_url_env)

redshift_url_env = os.getenv("REDSHIFT_URL")
parsed_redshift = urlparse(redshift_url_env)


# --- Define connection URLs ---
# PostgreSQL (Source) connection URL
postgres_url = URL.create(
    drivername="postgresql+psycopg2",
    host=parsed_postgres.hostname,
    port=parsed_postgres.port or "5432",
    database=parsed_postgres.path.lstrip("/"),
    username=parsed_postgres.username,
    password=parsed_postgres.password
)

# Amazon Redshift (Target) connection URL
redshift_url = URL.create(
    drivername="redshift+redshift_connector",
    host=os.getenv("REDSHIFT_HOST"),
    port=int(os.getenv("REDSHIFT_PORT") or "5439"),
    database=os.getenv("REDSHIFT_DB"),
    username=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD"))


print("PostgreSQL URL:", postgres_url)
print("Redshift URL:", redshift_url)
# --- Create Engines ---
# Engine for the PostgreSQL source
pg_engine = sa.create_engine(
    postgres_url,
    pool_size=10,
    pool_timeout=100,
    pool_recycle=900,
    max_overflow=5,
    pool_pre_ping=True,
    echo=True
)

# Engine for the Amazon Redshift target
rs_engine = sa.create_engine(
    redshift_url,
    pool_size=20,
    pool_timeout=150,
    pool_recycle=900,
    max_overflow=10,
    pool_pre_ping=True
)

# Optionally, set up Flask-SQLAlchemy for Redshift (for use in other routes)
app.config['SQLALCHEMY_DATABASE_URI'] = str(redshift_url)
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_size': 20,
    'pool_timeout': 150,
    'pool_recycle': 900,
    'max_overflow': 10,
    'pool_pre_ping': True
}
# (This instance 'db' can be used for ORM models if needed.)
db = SQLAlchemy(app)

# --- Data Aggregation Function ---
def aggregate_sales_data():
    print("Aggregating sales data from PostgreSQL and saving to Redshift...")   
    # Use SQLAlchemy connections for both source and target.
    with pg_engine.connect() as pg_conn, rs_engine.begin() as rs_conn: 
    # Ensure the best_sales_teams table exists
        # 1. Best Performing Sales Teams
        query_top_teams = sa.text("""
            SELECT a.branch, COUNT(*) AS total_sales, SUM(s.amount) AS total_revenue , a.name AS agent_name
            FROM sale s
            JOIN agent a ON s.agent_id = a.agent_id
            GROUP BY a.name,a.branch
            ORDER BY total_revenue DESC
            LIMIT 5;
        """)
        top_teams = pg_conn.execute(query_top_teams).fetchall()

        # Clear destination table
        rs_conn.execute(sa.text("DELETE FROM best_sales_teams;"))
        for branch, total_sales, total_revenue,agent_name in top_teams:
            rs_conn.execute(
                sa.text("""
                    INSERT INTO best_sales_teams (branch, total_sales, total_revenue)
                    VALUES (:branch, :total_sales, :total_revenue);
                """),
                {'branch': branch, 'total_sales': total_sales, 'total_revenue': total_revenue, 'agent_name': agent_name}
            )
        print("Best Performing Sales Teams aggregated.")

        # 2. Products Achieving Sales Targets (>10000)
        query_top_products = sa.text("""
            SELECT s.product AS product_name, SUM(s.amount) AS total_sales, a.branch AS branch
            FROM sale s
            JOIN agent a ON s.agent_id = a.agent_id
            GROUP BY a.branch,s.product
            HAVING SUM(s.amount) > 1000
            ORDER BY total_sales DESC;
        """)
        top_products = pg_conn.execute(query_top_products).fetchall()

        rs_conn.execute(sa.text("DELETE FROM top_selling_products;"))
        for  product_name, total_sales ,branch in top_products:
            rs_conn.execute(
                sa.text("""
                    INSERT INTO top_selling_products (branch, product_name, total_sales)
                    VALUES (:branch,:product_name, :total_sales);
                """),
                { 'branch': branch,'product_name': product_name, 'total_sales': total_sales}
            )
        print("Products achieving sales targets aggregated.")

        # 3. Branch-wise Sales Performance
        query_branch_performance = sa.text("""
            SELECT a.branch, SUM(s.amount) AS total_sales
            FROM sale s
            JOIN agent a ON s.agent_id = a.agent_id
            GROUP BY a.branch
            ORDER BY total_sales DESC;
        """)
        branch_performance = pg_conn.execute(query_branch_performance).fetchall()

        rs_conn.execute(sa.text("DELETE FROM branch_sales_performance;"))
        for branch, total_sales in branch_performance:
            rs_conn.execute(
                sa.text("""
                    INSERT INTO branch_sales_performance (branch, total_sales)
                    VALUES (:branch, :total_sales);
                """),
                {'branch': branch, 'total_sales': total_sales}
            )
        print("Branch-wise sales performance aggregated.")

    print("Data aggregation completed and saved to Redshift successfully!")

@app.route('/')
def index():
    try:
        aggregate_sales_data()
        return "Data aggregation completed successfully!"
    except Exception as e:
        return f"Error during aggregation: {e}", 500

if __name__ == '__main__':
    index()
