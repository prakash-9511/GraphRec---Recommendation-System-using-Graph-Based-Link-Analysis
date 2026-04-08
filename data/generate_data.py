"""
generate_data.py  –  Synthetic Amazon-style review dataset
Outputs: data/reviews.csv, data/products.csv, data/users.csv
"""
import csv, random, os

random.seed(42)

CATEGORIES = ["Electronics","Books","Clothing","Home","Sports","Toys","Beauty","Kitchen"]

PRODUCT_NAMES = {
    "Electronics": ["Bluetooth Speaker","Wireless Earbuds","Smart Watch","USB Hub","Laptop Stand","Webcam","LED Strip","Power Bank"],
    "Books":        ["Python Programming","Data Structures","Machine Learning","Clean Code","Deep Work","Atomic Habits","The Pragmatic Programmer","System Design"],
    "Clothing":     ["Cotton T-Shirt","Denim Jacket","Running Shoes","Formal Shirt","Sports Shorts","Hoodie","Sneakers","Polo Shirt"],
    "Home":         ["Air Purifier","Table Lamp","Wall Clock","Curtains","Bed Sheets","Cushion Cover","Storage Box","Photo Frame"],
    "Sports":       ["Yoga Mat","Dumbbells","Resistance Bands","Skipping Rope","Water Bottle","Gym Gloves","Foam Roller","Pull-up Bar"],
    "Toys":         ["LEGO Set","RC Car","Board Game","Puzzle","Action Figure","Stuffed Animal","Building Blocks","Card Game"],
    "Beauty":       ["Face Wash","Sunscreen","Lip Balm","Hair Serum","Moisturizer","Eye Cream","Body Lotion","Face Mask"],
    "Kitchen":      ["Air Fryer","Coffee Maker","Blender","Toaster","Knife Set","Non-Stick Pan","Rice Cooker","Electric Kettle"],
}

NUM_USERS=200; NUM_PRODUCTS=100; NUM_REVIEWS=2000

products=[]
pid=1
for cat, names in PRODUCT_NAMES.items():
    for name in names:
        products.append({"product_id":f"P{pid:04d}","product_name":f"{name}","category":cat,"price":round(random.uniform(5,500),2)})
        pid+=1
        if pid>NUM_PRODUCTS+1: break
    if pid>NUM_PRODUCTS+1: break

os.makedirs(os.path.dirname(os.path.abspath(__file__)), exist_ok=True)

with open(os.path.join(os.path.dirname(__file__),"products.csv"),"w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=["product_id","product_name","category","price"]); w.writeheader(); w.writerows(products)

users=[{"user_id":f"U{i:04d}","username":f"user_{i}"} for i in range(1,NUM_USERS+1)]
with open(os.path.join(os.path.dirname(__file__),"users.csv"),"w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=["user_id","username"]); w.writeheader(); w.writerows(users)

user_prefs={u["user_id"]:random.sample(CATEGORIES,k=random.randint(2,4)) for u in users}
reviews=[]; seen=set(); attempts=0
while len(reviews)<NUM_REVIEWS and attempts<NUM_REVIEWS*10:
    attempts+=1
    user=random.choice(users)["user_id"]
    cat_products=[p for p in products if p["category"] in user_prefs[user]] if random.random()<0.7 else products
    prod=random.choice(cat_products)["product_id"]
    if (user,prod) in seen: continue
    seen.add((user,prod))
    reviews.append({"review_id":f"R{len(reviews)+1:05d}","user_id":user,"product_id":prod,"rating":random.choices([1,2,3,4,5],weights=[5,10,15,35,35])[0],"helpful_votes":random.randint(0,50),"verified":random.choice(["True","False"])})

with open(os.path.join(os.path.dirname(__file__),"reviews.csv"),"w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=["review_id","user_id","product_id","rating","helpful_votes","verified"]); w.writeheader(); w.writerows(reviews)

print(f"Generated {len(products)} products, {len(users)} users, {len(reviews)} reviews.")
