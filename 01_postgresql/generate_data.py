"""
Retail360 Demo — Data Generation Script (Azure PostgreSQL)
==========================================================
Generates all source data for the Retail360 Fabric demo.

Tables created in PostgreSQL:
  stores, products, orders, order_items, inventory

External files generated in ./output_files/:
  weather/            daily JSON files (one per day)
  promotions/         promo_calendar.csv (English names)
  competitor_prices/  monthly CSV files

Usage:
  1. cp config/config.example.json config/config.json
  2. Fill in PostgreSQL credentials in config.json
  3. pip install -r config/requirements.txt
  4. python 01_postgresql/generate_data.py

Duration: ~15-25 minutes for 2M+ rows
"""

import json, os, sys, csv, math, random
from datetime import date, datetime, timedelta
from collections import defaultdict

import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
if not os.path.exists(config_path):
    print("ERROR: config/config.json not found.")
    print("Copy config/config.example.json to config/config.json and fill in values.")
    sys.exit(1)

with open(config_path) as f:
    CONFIG = json.load(f)

PG  = CONFIG["postgresql"]
DG  = CONFIG["data_generation"]

SEED         = DG["random_seed"]
START_DATE   = date.fromisoformat(DG["start_date"])
END_DATE     = date.fromisoformat(DG["end_date"])
N_STORES     = DG["n_stores"]
N_PRODUCTS   = DG["n_products"]
TARGET       = DG["target_orders"]
OUTPUT_DIR   = DG["output_dir"]
BATCH        = 5000

random.seed(SEED); np.random.seed(SEED)
fake = Faker("fr_FR"); Faker.seed(SEED)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Reference data ────────────────────────────────────────────

REGIONS = {
    "Île-de-France":              {"cities":["Paris","Boulogne-Billancourt","Saint-Denis","Montreuil","Versailles","Créteil","Argenteuil","Nanterre"], "w":0.22},
    "Auvergne-Rhône-Alpes":       {"cities":["Lyon","Grenoble","Saint-Étienne","Clermont-Ferrand","Valence","Annecy","Chambéry"],                     "w":0.14},
    "Nouvelle-Aquitaine":         {"cities":["Bordeaux","Limoges","Pau","Bayonne","Agen","Périgueux","Angoulême"],                                     "w":0.11},
    "Occitanie":                  {"cities":["Toulouse","Montpellier","Nîmes","Perpignan","Narbonne","Albi","Rodez"],                                  "w":0.12},
    "Hauts-de-France":            {"cities":["Lille","Amiens","Roubaix","Tourcoing","Calais","Dunkerque","Valenciennes"],                              "w":0.10},
    "Grand Est":                  {"cities":["Strasbourg","Metz","Reims","Nancy","Mulhouse","Colmar","Troyes"],                                        "w":0.09},
    "Normandie":                  {"cities":["Rouen","Caen","Le Havre","Évreux","Cherbourg","Alençon"],                                               "w":0.06},
    "Bretagne":                   {"cities":["Rennes","Brest","Nantes","Quimper","Lorient","Saint-Malo"],                                             "w":0.07},
    "Provence-Alpes-Côte d'Azur": {"cities":["Marseille","Nice","Toulon","Aix-en-Provence","Avignon","Cannes"],                                       "w":0.09},
}

CATALOG = {
    "Alimentation":         {"sub":["Épicerie sèche","Conserves","Boissons","Confiseries","Produits bio"],       "p":(1.5,45),    "m":(0.18,0.35),"w":0.20},
    "Électronique":         {"sub":["Smartphones","Tablettes","Accessoires","Audio","Gaming"],                    "p":(15,1200),   "m":(0.08,0.22),"w":0.15},
    "Maison & Décoration":  {"sub":["Literie","Rangement","Décoration","Luminaires","Textiles maison"],          "p":(8,350),     "m":(0.30,0.55),"w":0.12},
    "Mode & Textile":       {"sub":["Vêtements femme","Vêtements homme","Enfant","Chaussures","Accessoires"],    "p":(10,180),    "m":(0.40,0.65),"w":0.14},
    "Sports & Loisirs":     {"sub":["Fitness","Outdoor","Sports collectifs","Vélo","Natation"],                  "p":(5,600),     "m":(0.25,0.45),"w":0.10},
    "Jardin & Bricolage":   {"sub":["Outils","Jardinage","Peinture","Plomberie","Électricité"],                  "p":(3,400),     "m":(0.28,0.48),"w":0.08},
    "Beauté & Santé":       {"sub":["Soins visage","Maquillage","Parfums","Hygiène","Compléments"],              "p":(3,120),     "m":(0.35,0.60),"w":0.09},
    "Jouets & Enfants":     {"sub":["Jouets 0-3 ans","Jeux de société","Jeux éducatifs","Plein air","Figurines"],"p":(5,150),     "m":(0.32,0.52),"w":0.07},
    "Librairie & Papeterie":{"sub":["Romans","BD & Mangas","Scolaire","Papeterie","Art"],                        "p":(2,50),      "m":(0.22,0.38),"w":0.03},
    "Auto & Moto":          {"sub":["Entretien","Équipement","Accessoires","GPS","Sécurité"],                    "p":(5,300),     "m":(0.25,0.45),"w":0.02},
}

CHANNELS    = ["in_store","click_and_collect","home_delivery","drive"]
CH_WEIGHTS  = [0.55,0.18,0.20,0.07]
MONTH_SEAS  = {1:.75,2:.70,3:.85,4:.90,5:.95,6:1.0,7:.95,8:.90,9:1.0,10:1.05,11:1.35,12:1.60}

PROMOS = [
    {"name":"Winter Sales",     "s":"01-10","e":"02-07","d":0.30,"b":1.40},
    {"name":"Valentine's Day",  "s":"02-10","e":"02-14","d":0.10,"b":1.15},
    {"name":"Summer Sales",     "s":"06-28","e":"07-26","d":0.30,"b":1.35},
    {"name":"Back to School",   "s":"08-15","e":"09-10","d":0.10,"b":1.20},
    {"name":"Black Friday",     "s":"11-28","e":"11-28","d":0.25,"b":2.80},
    {"name":"Cyber Monday",     "s":"12-01","e":"12-01","d":0.20,"b":1.80},
    {"name":"Christmas",        "s":"12-20","e":"12-24","d":0.15,"b":2.20},
]

CLIMATE = {
    "Île-de-France":              {"t":12,"s":8, "r":0.40},
    "Auvergne-Rhône-Alpes":       {"t":11,"s":10,"r":0.38},
    "Nouvelle-Aquitaine":         {"t":14,"s":7, "r":0.42},
    "Occitanie":                  {"t":15,"s":8, "r":0.32},
    "Hauts-de-France":            {"t":10,"s":7, "r":0.48},
    "Grand Est":                  {"t":10,"s":9, "r":0.43},
    "Normandie":                  {"t":11,"s":6, "r":0.50},
    "Bretagne":                   {"t":12,"s":6, "r":0.52},
    "Provence-Alpes-Côte d'Azur": {"t":16,"s":7, "r":0.25},
}

COMPETITORS = {
    "MaxiMarché":   {"bias":-0.05,"std":0.08},
    "PrimoShop":    {"bias": 0.03,"std":0.06},
    "BonPrix":      {"bias":-0.10,"std":0.10},
    "NeoShop":      {"bias": 0.02,"std":0.09},
    "TendancePlus": {"bias":-0.02,"std":0.07},
}

# ── Helpers ───────────────────────────────────────────────────

def r2(x): return round(float(x), 2)

def date_range(s, e):
    d = s
    while d <= e:
        yield d; d += timedelta(days=1)

def get_promo(d):
    for p in PROMOS:
        s = date(d.year, int(p["s"][:2]), int(p["s"][3:]))
        e = date(d.year, int(p["e"][:2]), int(p["e"][3:]))
        if s <= d <= e: return p
    return None

def day_coef(d):
    c = MONTH_SEAS[d.month]
    if d.weekday() >= 5: c *= 1.25
    p = get_promo(d)
    if p: c *= p["b"]
    c *= 1 + 0.08 * (d - START_DATE).days / 365
    return c

# ── DDL ───────────────────────────────────────────────────────

DDL = """
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders      CASCADE;
DROP TABLE IF EXISTS inventory   CASCADE;
DROP TABLE IF EXISTS products    CASCADE;
DROP TABLE IF EXISTS stores      CASCADE;

CREATE TABLE stores (
    store_id     INT PRIMARY KEY,
    name         VARCHAR(150) NOT NULL,
    city         VARCHAR(80)  NOT NULL,
    region       VARCHAR(80)  NOT NULL,
    address      VARCHAR(200),
    postal_code  VARCHAR(10),
    surface_m2   INT NOT NULL,
    opening_date DATE NOT NULL,
    store_type   VARCHAR(30),
    manager_name VARCHAR(100),
    phone        VARCHAR(30)
);

CREATE TABLE products (
    product_id   INT PRIMARY KEY,
    name         VARCHAR(150) NOT NULL,
    category     VARCHAR(60)  NOT NULL,
    subcategory  VARCHAR(60)  NOT NULL,
    ean          VARCHAR(13),
    unit_cost    NUMERIC(10,2) NOT NULL,
    unit_price   NUMERIC(10,2) NOT NULL,
    weight_kg    NUMERIC(8,2),
    is_active    BOOLEAN DEFAULT TRUE,
    launch_date  DATE
);

CREATE TABLE orders (
    order_id     INT PRIMARY KEY,
    store_id     INT NOT NULL REFERENCES stores(store_id),
    order_date   DATE NOT NULL,
    customer_id  INT,
    channel      VARCHAR(30),
    order_status VARCHAR(20)
);
CREATE INDEX idx_ord_date  ON orders(order_date);
CREATE INDEX idx_ord_store ON orders(store_id);
CREATE INDEX idx_ord_cust  ON orders(customer_id);

CREATE TABLE order_items (
    item_id      BIGINT PRIMARY KEY,
    order_id     INT NOT NULL REFERENCES orders(order_id),
    product_id   INT NOT NULL REFERENCES products(product_id),
    quantity     SMALLINT NOT NULL,
    unit_price   NUMERIC(10,2) NOT NULL,
    discount_pct NUMERIC(5,2) DEFAULT 0,
    line_total   NUMERIC(12,2) NOT NULL
);
CREATE INDEX idx_itm_ord  ON order_items(order_id);
CREATE INDEX idx_itm_prod ON order_items(product_id);

CREATE TABLE inventory (
    inv_id           BIGINT PRIMARY KEY,
    store_id         INT NOT NULL REFERENCES stores(store_id),
    product_id       INT NOT NULL REFERENCES products(product_id),
    quantity_on_hand INT NOT NULL,
    safety_stock     INT DEFAULT 0,
    reorder_point    INT DEFAULT 0,
    stock_status     VARCHAR(20),
    last_updated     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (store_id, product_id)
);
CREATE INDEX idx_inv_store ON inventory(store_id);
CREATE INDEX idx_inv_prod  ON inventory(product_id);
"""

# ── Generators ────────────────────────────────────────────────

def gen_stores():
    print(f"\n[1/7] Generating {N_STORES} stores...")
    stores, sid = [], 1
    rnames = list(REGIONS.keys())
    rw     = [REGIONS[r]["w"] for r in rnames]
    counts = defaultdict(int)
    for _ in range(N_STORES):
        counts[random.choices(rnames, weights=rw)[0]] += 1
    adjs = ["nord","sud","centre","ouest","est","avenue","rue","place","parc","grand"]
    for region, cnt in counts.items():
        for _ in range(cnt):
            surf = random.choice([450,600,800,1200,1800,2500,4000,6000])
            city = random.choice(REGIONS[region]["cities"])
            stores.append({
                "store_id":sid,"name":f"Retail360 {city} {random.choice(adjs).capitalize()}",
                "city":city,"region":region,"address":fake.street_address(),
                "postal_code":fake.postcode(),"surface_m2":surf,
                "opening_date":fake.date_between(date(2010,1,1),date(2021,12,31)),
                "store_type":"hypermarket" if surf>3000 else ("supermarket" if surf>1000 else "convenience"),
                "manager_name":fake.name(),"phone":fake.phone_number(),
            })
            sid += 1
    return stores

def gen_products():
    print(f"[2/7] Generating {N_PRODUCTS} products...")
    products, pid = [], 1
    cats = list(CATALOG.keys())
    cw   = [CATALOG[c]["w"] for c in cats]
    counts = defaultdict(int)
    for _ in range(N_PRODUCTS):
        counts[random.choices(cats, weights=cw)[0]] += 1
    adjs  = ["premium","ultra","eco","smart","classic","elite","new","original","compact","deluxe"]
    nouns = ["select","boost","line","plus","max","expert","vision","style","care","tech"]
    for cat, cnt in counts.items():
        info = CATALOG[cat]
        for _ in range(cnt):
            up = r2(random.uniform(*info["p"]))
            uc = r2(up * (1 - random.uniform(*info["m"])))
            sub = random.choice(info["sub"])
            products.append({
                "product_id":pid,
                "name":f"{random.choice(adjs).capitalize()} {sub} {random.choice(nouns).capitalize()} {pid}"[:150],
                "category":cat,"subcategory":sub,"ean":fake.ean(length=13),
                "unit_cost":uc,"unit_price":up,
                "weight_kg":r2(random.uniform(0.1,25.0)),
                "is_active":random.random()>0.05,
                "launch_date":fake.date_between(date(2015,1,1),date(2022,6,1)),
            })
            pid += 1
    return products

def gen_orders_items(stores, products):
    print(f"[3/7] Generating ~{TARGET:,} orders...")
    all_dates  = list(date_range(START_DATE, END_DATE))
    opd        = TARGET / len(all_dates)
    store_ids  = [s["store_id"]   for s in stores]
    store_w    = [s["surface_m2"] for s in stores]
    active     = [p for p in products if p["is_active"]]
    n_active   = len(active)
    pw         = [1/(i+1)**0.7 for i in range(n_active)]
    total_pw   = sum(pw); pw = [x/total_pw for x in pw]
    orders, items, oid, iid = [], [], 1, 1
    for d in tqdm(all_dates, desc="  days"):
        coef = day_coef(d)
        for _ in range(max(1, int(np.random.poisson(opd * coef)))):
            sid = random.choices(store_ids, weights=store_w)[0]
            ch  = random.choices(CHANNELS, weights=CH_WEIGHTS)[0]
            cid = random.randint(1, 500_000)
            orders.append((oid, sid, d, cid, ch, "completed"))
            promo = get_promo(d)
            n_it  = min(15, max(1, int(np.random.geometric(p=0.40))))
            for prod in np.random.choice(active, size=min(n_it,n_active), replace=False, p=pw):
                qty  = random.randint(1,4)
                disc = promo["d"]*random.uniform(0.5,1.0) if promo else (
                       random.uniform(0,0.05) if ch=="home_delivery" else (
                       random.choice([0.05,0.10,0.15,0.20]) if random.random()<0.12 else 0.0))
                items.append((iid, oid, prod["product_id"], qty,
                              r2(prod["unit_price"]), r2(disc*100),
                              r2(qty*prod["unit_price"]*(1-disc))))
                iid += 1
            oid += 1
    print(f"  → {len(orders):,} orders | {len(items):,} items")
    return orders, items

def gen_inventory(stores, products):
    print("[4/7] Generating inventory...")
    inv, iid = [], 1
    now = datetime.now()
    for store in tqdm(stores, desc="  stores"):
        surf_f = math.log(store["surface_m2"]/400+1)
        n_prods = int(len(products)*random.uniform(0.60,0.85))
        for prod in random.sample(products, n_prods):
            if not prod["is_active"]: continue
            base = max(5, int(np.random.gamma(5,20)*surf_f))
            ss   = max(1, int(base*0.20))
            rp   = max(2, int(base*0.35))
            rnd  = random.random()
            if rnd < 0.03:   qty,st = 0,"stockout"
            elif rnd < 0.11: qty,st = random.randint(1,ss),"critical"
            elif rnd < 0.23: qty,st = random.randint(ss+1,max(ss+2,rp)),"low"
            else:            qty,st = random.randint(rp+1,base*3),"normal"
            inv.append((iid, store["store_id"], prod["product_id"],
                        qty, ss, rp, st, now-timedelta(hours=random.randint(0,72))))
            iid += 1
    print(f"  → {len(inv):,} rows")
    return inv

# ── PostgreSQL loaders ────────────────────────────────────────

def load_pg(conn, stores, products, orders, items, inventory):
    print("\n[5/7] Loading PostgreSQL...")
    with conn.cursor() as cur: cur.execute(DDL)
    conn.commit()
    def bulk(table, rows, batch=BATCH):
        with conn.cursor() as cur:
            for i in tqdm(range(0,len(rows),batch), desc=f"  {table}"):
                execute_values(cur, f"INSERT INTO {table} VALUES %s",
                               rows[i:i+batch], page_size=batch)
                conn.commit()
    bulk("stores",      [(s["store_id"],s["name"],s["city"],s["region"],
                          s["address"],s["postal_code"],s["surface_m2"],
                          s["opening_date"],s["store_type"],
                          s["manager_name"],s["phone"]) for s in stores])
    bulk("products",    [(p["product_id"],p["name"],p["category"],p["subcategory"],
                          p["ean"],p["unit_cost"],p["unit_price"],p["weight_kg"],
                          p["is_active"],p["launch_date"]) for p in products])
    bulk("orders",      orders)
    bulk("order_items", items)
    bulk("inventory",   inventory)
    print("  ✓ PostgreSQL complete")

# ── External files ────────────────────────────────────────────

def gen_weather(stores):
    print("[6/7] Generating weather JSON files...")
    import json as _json
    wdir = os.path.join(OUTPUT_DIR,"weather"); os.makedirs(wdir, exist_ok=True)
    city_region = {s["city"]:s["region"] for s in stores}
    for d in tqdm(date_range(START_DATE, END_DATE), desc="  days"):
        mc    = math.sin(math.pi*(d.month-1)/11)
        recs  = []
        for city,region in city_region.items():
            cl    = CLIMATE.get(region, {"t":12,"s":8,"r":0.40})
            t_avg = cl["t"] + 14*mc
            tmax  = round(float(np.random.normal(t_avg+3, cl["s"]*0.5)),1)
            tmin  = round(float(np.random.normal(t_avg-3, cl["s"]*0.5)),1)
            rain  = random.random() < cl["r"]
            prec  = round(float(np.random.exponential(5.0)),1) if rain else 0.0
            wind  = round(float(np.random.gamma(2,8)),1)
            cond  = "pluie" if prec>5 else ("nuageux" if prec>0 else ("ensoleillé" if tmax>20 else "couvert"))
            recs.append({"date":str(d),"city":city,"region":region,
                         "temp_max_c":tmax,"temp_min_c":tmin,
                         "precipitation_mm":prec,"wind_kmh":wind,
                         "is_sunny": not rain and tmax>18,
                         "weather_label":cond})
        with open(os.path.join(wdir,f"{d}.json"),"w",encoding="utf-8") as f:
            _json.dump(recs,f,ensure_ascii=False)
    print(f"  → {(END_DATE-START_DATE).days+1} files")

def gen_promotions():
    print("[7a/7] Generating promotions CSV...")
    pdir = os.path.join(OUTPUT_DIR,"promotions"); os.makedirs(pdir, exist_ok=True)
    cats = list(CATALOG.keys())
    rows, pid = [], 1
    for year in range(START_DATE.year, END_DATE.year+1):
        for evt in PROMOS:
            s = date(year, int(evt["s"][:2]), int(evt["s"][3:]))
            e = date(year, int(evt["e"][:2]), int(evt["e"][3:]))
            if s > END_DATE: continue
            is_major = any(k in evt["name"] for k in ["Black Friday","Christmas","Sales","Cyber"])
            for cat in (cats if is_major else random.sample(cats, random.randint(2,5))):
                rows.append({"promo_id":pid,"promo_name":evt["name"],
                             "start_date":str(s),"end_date":str(e),"category":cat,
                             "discount_pct":round(evt["d"]*100,1),"traffic_boost":evt["b"],
                             "is_national":True,"budget_eur":random.randint(10_000,500_000),
                             "communication_type":random.choice(["print+digital","digital","radio+digital","TV+digital"]),
                             "is_active": s<=date.today()<=e})
                pid += 1
    fpath = os.path.join(pdir,"promo_calendar.csv")
    with open(fpath,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)
    print(f"  → {len(rows)} rows")

def gen_competitor_prices(products):
    print("[7b/7] Generating competitor prices CSV files...")
    cdir = os.path.join(OUTPUT_DIR,"competitor_prices"); os.makedirs(cdir, exist_ok=True)
    monitored = random.sample(products, int(len(products)*0.30))
    cur  = START_DATE.replace(day=1)
    months = []
    while cur <= END_DATE:
        months.append(cur)
        cur = (cur.replace(day=28)+timedelta(days=4)).replace(day=1)
    for m in tqdm(months, desc="  months"):
        rows = []
        for prod in monitored:
            for comp,profile in COMPETITORS.items():
                gap  = np.random.normal(profile["bias"], profile["std"])
                cp   = max(0.50, r2(prod["unit_price"]*(1+gap)))
                gpct = r2((cp-prod["unit_price"])/prod["unit_price"]*100)
                rows.append({"snapshot_month":str(m)[:7],
                             "product_id":prod["product_id"],
                             "product_name":prod["name"][:80],
                             "category":prod["category"],
                             "competitor":comp,"competitor_price":cp,
                             "our_price":prod["unit_price"],
                             "price_gap_pct":gpct,"is_cheaper":gpct<0,
                             "source":random.choice(["web_scraping","partenaire","client_report"])})
        fname = f"competitor_prices_{m.year}_{m.month:02d}.csv"
        with open(os.path.join(cdir,fname),"w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader(); w.writerows(rows)
    print(f"  → {len(months)} CSV files")

# ── Main ──────────────────────────────────────────────────────

def main():
    print("="*58)
    print("  RETAIL360 — DATA GENERATOR")
    print(f"  Period  : {START_DATE} → {END_DATE}")
    print(f"  Stores  : {N_STORES}   |   Products : {N_PRODUCTS}")
    print(f"  Target  : ~{TARGET:,} orders")
    print("="*58)

    stores           = gen_stores()
    products         = gen_products()
    orders, items    = gen_orders_items(stores, products)
    inventory        = gen_inventory(stores, products)

    print(f"\n[5/7] Connecting to PostgreSQL: {PG['host']}...")
    try:
        conn = psycopg2.connect(**PG)
        conn.autocommit = False
        load_pg(conn, stores, products, orders, items, inventory)
        conn.close()
    except Exception as e:
        print(f"  ✗ PostgreSQL error: {e}")
        print("  → Generating external files only.")

    gen_weather(stores)
    gen_promotions()
    gen_competitor_prices(products)

    rev = sum(i[6] for i in items)
    print(f"\n{'─'*58}")
    print("  GENERATION COMPLETE")
    print(f"{'─'*58}")
    print(f"  Stores       : {len(stores):>10,}")
    print(f"  Products     : {len(products):>10,}")
    print(f"  Orders       : {len(orders):>10,}")
    print(f"  Order items  : {len(items):>10,}")
    print(f"  Inventory    : {len(inventory):>10,}")
    print(f"  Total revenue: {rev:>10,.0f} €")
    print(f"{'─'*58}")
    print("  External files → ./output_files/")
    print("    Upload to Files/raw/ in retail360_lh")
    print(f"{'─'*58}")

if __name__ == "__main__":
    main()
