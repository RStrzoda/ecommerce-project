import os
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DBT_DIR = os.getenv("DBT_DIR")

def render_report_text():
    sales_df = pd.read_csv(f"{DBT_DIR}seeds\orders.csv")
    products_df = pd.read_csv(f"{DBT_DIR}seeds\products.csv")

    total_pedidos = len(sales_df)
    total_cancelamentos = len(sales_df[sales_df['status'] == 'cancelado'])
    taxa_cancelamento = round((total_cancelamentos / total_pedidos) * 100 , 2)
    top_produto = products_df.sort_values(by='price', ascending=False).iloc[0]["name"]


    message = (
        " * Relatório Semanal - E-commerce* \n\n"
        f"- Total de pedidos: *{total_pedidos}\n"
        f"- Pedidos cancelados: *{total_cancelamentos}*\n"
        f"- Taxa de cancelamento: *{taxa_cancelamento}%*\n"
        f"- Produto mais caro: *{top_produto}*\n" 
    )
    return message

def send_report(message:str):
    """Envia relatorio para telegram pela api"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    response = requests.post(url, json=payload)
    if response.status_code != 200:
        print(f"Erro ao enviar mensgem: {response.status_code} - {response.text}")
        response.raise_for_status()
    else:
        print("Relatório enviado com sucesso")

def notify_telegram_weekly_report():
    report_text = render_report_text()
    send_report(report_text)


if __name__ == "__main__":
    notify_telegram_weekly_report()