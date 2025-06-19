import os
import base64
import json
import time
import random
import pandas as pd
from kafka import KafkaProducer
from icrawler.builtin import GoogleImageCrawler

df = pd.read_csv(os.path.join('data', 'ecommerce-behavior-data.csv'))
categories = df['category_code'].dropna().unique()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    max_request_size=5 * 1024 * 1024  
)

def crawl_all_images():
    for category in categories:
        print(f"Crawling: {category}")
        folder = os.path.join("data", "images", category.replace('.', '_'))
        if os.path.exists(folder) and len(os.listdir(folder)) >= 1:
            print(f"Skip {category}, already crawled.")
            continue 

        os.makedirs(folder, exist_ok=True)
        crawler = GoogleImageCrawler(storage={'root_dir': folder})
        try:
            crawler.crawl(keyword=category, max_num=1)
        except Exception as e:
            print(f"[!] Error crawling {category}: {e}")

def stream_all_images():
    for category in categories:
        folder = os.path.join("data", "images", category.replace('.', '_'))
        if not os.path.exists(folder): continue
        for img_file in os.listdir(folder):
            try:
                with open(os.path.join(folder, img_file), "rb") as f:
                    encoded_img = base64.b64encode(f.read()).decode("utf-8")
                message = {
                    "category": category,
                    "image_name": img_file,
                    "image_data": encoded_img
                }
                producer.send("ecommerce-images", json.dumps(message).encode("utf-8"))
                print(f"Sent {img_file} from {category}")

                time.sleep(random.uniform(0.01, 0.1))

            except Exception as e:
                print(f"[!] Failed to send {img_file}: {e}")
    producer.flush()

# --- Main Pipeline ---
if __name__ == "__main__":
    print("\n=== CRAWLING PHASE ===\n")
    crawl_all_images()

    print("\n=== STREAMING PHASE ===\n")
    stream_all_images()

    print("\nAll Done.")
