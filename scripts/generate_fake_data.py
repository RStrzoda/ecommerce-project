import pandas as pd
from faker import Faker
import random
from dotenv import load_dotenv
import os
from pathlib import Path
from datetime import timedelta

#Variaveis de ambiente
load_dotenv()

# Gerador de dados
fake = Faker()
random.seed(42)

#Variáveis
N_CUSTOMERS = int(os.getenv("N_CUSTOMERS"))
N_PRODUCTS = int(os.getenv("N_PRODUCTS"))
N_ORDERS = int(os.getenv("N_ORDERS"))
OUTPUT_PATH = os.getenv("OUTPUT_PATH")

OUTPUT_PATH = Path(OUTPUT_PATH )

# -------------- 1. Clientes ---------------------

customers = []

for i in range(1, N_CUSTOMERS +1):
    customers.append({
        "customer_id": i,
        "name": fake.name(),
        "email": fake.email(),
        "signup_date": fake.date_between(start_date="-2y", end_date="today")
    })

df_customers = pd.DataFrame(customers)
df_customers.to_csv(OUTPUT_PATH/"customers.csv", index=False)

# -------------- 2. Produtos ---------------------

categories = ['Eletrônicos', 'Roupas', 'Livros', 'Esportes', 'Beleza']

products = []

for i in range(1, N_PRODUCTS + 1):
    categoria = random.choice(categories)
    custo = round(random.uniform(5,100), 2)
    preco = round(custo *random.uniform(1.2,2.5), 2)
    products.append({
        "product_id":  i,
        "name":fake.word().capitalize() + " " + fake.word().capitalize(),
        "category": categoria,
        "custo": custo,
        "price": preco
    })

df_products = pd.DataFrame(products)
df_products.to_csv(OUTPUT_PATH/"products.csv", index=False)

# -------------- 3. Pedidos e itens ---------------------

orders = []
order_items = []
for i in range(1, N_ORDERS + 1):
    customer_id = random.randint(1, N_CUSTOMERS)
    order_date = fake.date_between(start_date='-1y', end_date="today")
    status = random.choices(["completo", "cancelado"], weights=[0.85, 0.15])[0]

    orders.append({
        "order_id": i,
        "customer_id": customer_id,
        "order_date": order_date,
        "status": status
    })

    n_items = random.randint(1,4)
    for _ in range(n_items):
        product_id = random.randint(1, N_PRODUCTS)
        quantity = random.randint(1, 3)
        order_items.append({
            "order_id": i,
            "product_id": product_id,
            "quantity": quantity
        })

df_orders = pd.DataFrame(orders)
df_orders.to_csv(OUTPUT_PATH/"orders.csv", index=False)

df_order_items = pd.DataFrame(order_items)
df_order_items.to_csv(OUTPUT_PATH/"order_items.csv", index=False)

# -------------- 4. cancelamentos ---------------------

cancelamentos = df_orders[df_orders["status"]== "cancelado"].copy()
cancelamentos["cancel_reason"] = [fake.sentence(nb_words=4) for _ in range(len(cancelamentos))]
cancelamentos["cancel_date"] = cancelamentos["order_date"].apply(lambda d: d + timedelta(days= random.randint(1,3)))

df_canc = cancelamentos[["order_id", "cancel_reason", "cancel_date"]]
df_canc.to_csv(OUTPUT_PATH/"cancelamentos.csv", index = False)

print("DADOS GERADOS EM: ", OUTPUT_PATH.resolve())