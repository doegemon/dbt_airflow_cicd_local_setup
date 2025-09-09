import os
import uuid
import time
import duckdb
import random
import numpy as np
import pandas as pd
from faker import Faker

# Initial setup
start_time = time.time()
fake = Faker("pt_BR")
random.seed(42)
np.random.seed(42)

# Paths
SEEDS_PATH = "./seeds/"
os.makedirs(SEEDS_PATH, exist_ok=True)
DB_PATH = os.path.join(SEEDS_PATH, "data.duckdb")

# DuckDB connection
con = duckdb.connect(DB_PATH)


def create_tables_duck():
    """Function that creates tables in the DuckDB database"""
    con.execute("""
    CREATE TABLE IF NOT EXISTS registrations (
        id UUID PRIMARY KEY,
        name VARCHAR,
        birth_date DATE,
        cpf VARCHAR(14) UNIQUE,
        zipcode VARCHAR(9),
        city VARCHAR(100),
        state VARCHAR(2),
        country VARCHAR(50),
        gender CHAR(1),
        phone_number VARCHAR(20),
        email VARCHAR(100) UNIQUE,
        registration_date DATE
    )
    """)
    
    con.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id UUID PRIMARY KEY,
        cpf VARCHAR(14),
        order_value DECIMAL(10,2),
        freight_value DECIMAL(10,2),
        discount_value DECIMAL(10,2),
        coupon VARCHAR(50),
        delivery_address_street VARCHAR(100),
        delivery_address_number VARCHAR(10),
        delivery_address_neighborhood VARCHAR(100),
        delivery_address_city VARCHAR(100),
        delivery_address_state CHAR(2),
        delivery_address_country VARCHAR(50),
        order_status VARCHAR(30),
        order_date DATE,
        FOREIGN KEY (cpf) REFERENCES registrations(cpf)
    )
    """)


def get_existing_cpfs():
    """Returns all CPFs (personal identification document) in the registrations table"""
    result = con.execute("SELECT DISTINCT cpf FROM registrations").fetchall()
    return {row[0] for row in result} if result else set()


def generate_registration_batch(batch_size):
    """Generates a batch of registration data using Faker and returns a DataFrame"""
    print(f"Generating {batch_size} registrations")
    
    # Generate the data in smaller batches to avoid memory overload
    chunk_size = 10000
    chunks = []
    
    for chunk_start in range(0, batch_size, chunk_size):
        chunk_end = min(chunk_start + chunk_size, batch_size)
        chunk_data = []
        
        for _ in range(chunk_start, chunk_end):
            cpf = fake.bothify(text='###.###.###-##')
            chunk_data.append({
                'id': str(uuid.uuid4()),
                'name': fake.name(),
                'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
                'cpf': cpf,
                'zipcode': fake.postcode(),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'country': 'Brazil',
                'gender': random.choice(['M', 'F']),
                'phone_number': fake.phone_number(),
                'email': f"{cpf.replace('.', '').replace('-', '')}@fakeprovider.com.br",
                'registration_date': fake.date_between(start_date='-2y', end_date='today').isoformat()
            })
        
        # Creating a Dataframe with the current chunk
        df_chunk = pd.DataFrame(chunk_data)
        
        # Removing CPF duplicates from the current chunk
        df_chunk = df_chunk.drop_duplicates(subset=['cpf'])
        
        # Appending to the list of chunks
        chunks.append(df_chunk)
        
        print(f"{chunk_end}/{batch_size} registrations generated")
    
    # Concatenating all chunks in a single Dataframe
    if chunks:
        df = pd.concat(chunks, ignore_index=True)
        
        # Removing CPF duplicates among chunks
        df = df.drop_duplicates(subset=['cpf'])
        
        return df.head(batch_size)  # Assuring the size requested
    
    return pd.DataFrame()


def generate_orders_batch(cpfs, batch_size):
    """Generates a batch of orders for the CPFs generated and returns a DataFrame"""
    try:
        print(f"Generating {batch_size} orders")
        
        # Generate the data in smaller batches to avoid memory overload
        chunk_size = 10000
        chunks = []
        
        for chunk_start in range(0, batch_size, chunk_size):
            chunk_end = min(chunk_start + chunk_size, batch_size)
            chunk_data = []
            
            for _ in range(chunk_start, chunk_end):
                try:
                    # Selecting a random CPF
                    cpf = random.choice(cpfs)
                    
                    # Generating order data
                    total_value = round(random.uniform(50, 2000), 2)
                    has_discount = random.random() < 0.2  # 20% chance of having a discount
                    discount_value = round(total_value * random.uniform(0.05, 0.2), 2) if has_discount else 0.0
                    
                    # Generating an unique coupon code using UUID if the order has a discount
                    coupon = f"COUPON{str(uuid.uuid4())[:8].upper()}" if has_discount else None
                    
                    chunk_data.append({
                        'order_id': str(uuid.uuid4()),
                        'cpf': cpf,
                        'order_value': total_value,
                        'freight_value': round(random.uniform(5, 100), 2),
                        'discount_value': discount_value,
                        'coupon': coupon,
                        'delivery_address_street': fake.street_name(),
                        'delivery_address_number': fake.building_number(),
                        'delivery_address_neighborhood': fake.neighborhood(),
                        'delivery_address_city': fake.city(),
                        'delivery_address_state': fake.state_abbr(),
                        'delivery_address_country': 'Brazil',
                        'order_status': random.choice(['pending', 'paid', 'shipped', 'delivered', 'cancelled']),
                        'order_date': fake.date_between(start_date='-2y', end_date='today').isoformat()
                    })
                except Exception as e:
                    print(f"Error while generating order: {str(e)}")
                    continue
            
            if not chunk_data:
                continue
                
            try:
                # Creates a dataframe with the current chunk
                df_chunk = pd.DataFrame(chunk_data)
                chunks.append(df_chunk)
                
                print(f"{chunk_end}/{batch_size} orders generated")
                
            except Exception as e:
                print(f"Error creating DataFrame from chunk: {str(e)}")
                continue
        
        # Concatenate all chunks into a single DataFrame
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
            print(f"Total of {len(df)} orders successfully generated.")
            return df
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Critical error in generate_orders_batch: {str(e)}")
        return pd.DataFrame()


def batch_insert(table, df):
    """Insert data in batches using Pandas and DuckDB."""
    if df.empty:
        return
    
    # Converting to DuckDB
    con.register('temp_df', df)
    
    try:
        # Insert data ignoring duplicates
        con.execute(f"""
            INSERT OR IGNORE INTO {table} 
            SELECT * FROM temp_df
        """)
        
        # Count how many records have been inserted
        result = con.execute("SELECT COUNT(*) as inserted FROM temp_df").fetchone()[0]
        print(f"{result} records inserted into the {table} table")
        
    except Exception as e:
        print(f"Error when inserting data into the table {table}: {str(e)}")
        raise

    finally:
        # Remove the temporary DataFrame
        con.unregister('temp_df')


def export_to_csv():
    """Exports tables to .csv files."""
    con.execute(f"EXPORT DATABASE '{SEEDS_PATH}' (FORMAT CSV)")


def main():
    print("Starting data generation with DuckDB")
    
    try:
        # Create tables if they do not exist in the database
        create_tables_duck()
        
        # Ensure that the tables are empty
        con.execute("DELETE FROM orders")
        con.execute("DELETE FROM registrations")
        
        # Generating registrations
        print("Generating registrations")
        total_registrations = 10_000
        batch_registrations = 5_000
        
        for i in range(0, total_registrations, batch_registrations):
            current_size = min(batch_registrations, total_registrations - i)
            print(f"Processing registrations {i+1}-{i+current_size}")
            
            # Generating and inserting the registrations batch
            df_registrations = generate_registration_batch(current_size)
            if not df_registrations.empty:
                batch_insert('registrations', df_registrations)
        
        # Getting the CPFs of registered customers
        cpfs = con.execute("SELECT cpf FROM registrations").fetchdf()['cpf'].tolist()
        
        # Generating orders
        print("Generating orders")
        total_orders = 50_000
        batch_orders = 5_000
        
        for i in range(0, total_orders, batch_orders):
            print(f"Processing orders {i+1}-{min(i+batch_orders, total_orders)}")
            df_orders = generate_orders_batch(cpfs, min(batch_orders, total_orders - i))
            batch_insert('orders', df_orders)
        
        # simple data analysis
        print("\nBig Numbers:")
        n_registrations = con.execute("SELECT COUNT(*) FROM registrations").fetchone()[0]
        n_orders = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        avg_orders = n_orders / n_registrations if n_registrations > 0 else 0
        
        print(f"- Total registrations generated: {n_registrations:,}")
        print(f"- Total orders generated: {n_orders:,}")
        print(f"- Average orders per customer: {avg_orders:.2f}")
        
        # Exporting to .csv
        print("\nExporting to .csv")
        export_to_csv()
        
    finally:
        # Closing connection
        con.close()
        
        # Removing DuckDB temp file
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        
        # Removing temp files from seeds folder
        if os.path.exists(SEEDS_PATH + 'load.sql'):
            os.remove(SEEDS_PATH + 'load.sql')
        if os.path.exists(SEEDS_PATH + 'schema.sql'):
            os.remove(SEEDS_PATH + 'schema.sql')
    
    elapsed_time = time.time() - start_time
    print(f"\nTotal execution time: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    main()
