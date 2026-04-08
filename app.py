"""
app.py  –  Flask API + UI server for Project 36
Serves the web UI and exposes REST endpoints backed by PySpark.
"""

from flask import Flask, jsonify, request, render_template, send_from_directory
import sys, os, threading, time, json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

app = Flask(__name__, template_folder="templates", static_folder="static")

# ── Global Spark state ─────────────────────────────────────────────────────────
spark_state = {
    "status": "idle",       # idle | loading | ready | error
    "message": "Not started",
    "progress": 0,
    "spark": None,
    "edges": None,
    "product_graph": None,
    "pagerank": None,
    "products": None,
    "users": None,
    "reviews": None,
    "error": None,
}

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))


def init_spark_background():
    """Run Spark initialization in a background thread."""
    global spark_state
    try:
        from recommendation_engine import (
            create_spark, load_data, build_user_product_graph,
            build_product_graph, run_pagerank
        )

        spark_state["status"] = "loading"
        spark_state["message"] = "Starting Spark session..."
        spark_state["progress"] = 5

        spark = create_spark()
        spark_state["spark"] = spark
        spark_state["progress"] = 15
        spark_state["message"] = "Loading dataset..."

        reviews, products, users = load_data(spark, DATA_DIR)
        spark_state["reviews"]  = reviews
        spark_state["products"] = products
        spark_state["users"]    = users
        spark_state["progress"] = 35
        spark_state["message"]  = "Building user-product graph..."

        edges = build_user_product_graph(reviews)
        spark_state["edges"]    = edges
        spark_state["progress"] = 50
        spark_state["message"]  = "Building product co-interaction graph..."

        product_graph = build_product_graph(edges)
        spark_state["product_graph"] = product_graph
        spark_state["progress"] = 70
        spark_state["message"]  = "Running PageRank algorithm (10 iterations)..."

        pr = run_pagerank(product_graph, num_iterations=10, damping=0.85)
        spark_state["pagerank"] = pr
        spark_state["progress"] = 100
        spark_state["status"]   = "ready"
        spark_state["message"]  = "System ready"

    except Exception as e:
        spark_state["status"]  = "error"
        spark_state["error"]   = str(e)
        spark_state["message"] = f"Error: {e}"


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    return jsonify({
        "status":   spark_state["status"],
        "message":  spark_state["message"],
        "progress": spark_state["progress"],
    })


@app.route("/api/init", methods=["POST"])
def api_init():
    if spark_state["status"] in ("loading", "ready"):
        return jsonify({"ok": True, "message": spark_state["message"]})
    t = threading.Thread(target=init_spark_background, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Initialization started"})


@app.route("/api/stats")
def api_stats():
    if spark_state["status"] != "ready":
        return jsonify({"error": "System not ready"}), 503
    from recommendation_engine import get_graph_stats
    stats = get_graph_stats(spark_state["edges"], spark_state["product_graph"], spark_state["pagerank"])
    return jsonify(stats)


@app.route("/api/users")
def api_users():
    if spark_state["status"] != "ready":
        return jsonify({"error": "System not ready"}), 503
    rows = spark_state["users"].collect()
    return jsonify([r.asDict() for r in rows])


@app.route("/api/recommend/<user_id>")
def api_recommend(user_id):
    if spark_state["status"] != "ready":
        return jsonify({"error": "System not ready"}), 503
    top_n = int(request.args.get("top_n", 10))
    from recommendation_engine import get_recommendations
    recs = get_recommendations(
        user_id,
        spark_state["edges"],
        spark_state["product_graph"],
        spark_state["pagerank"],
        spark_state["products"],
        top_n=top_n,
    )
    return jsonify(recs)


@app.route("/api/history/<user_id>")
def api_history(user_id):
    if spark_state["status"] != "ready":
        return jsonify({"error": "System not ready"}), 503
    from recommendation_engine import get_user_history
    hist = get_user_history(user_id, spark_state["edges"], spark_state["products"])
    return jsonify(hist)


@app.route("/api/top-products")
def api_top_products():
    if spark_state["status"] != "ready":
        return jsonify({"error": "System not ready"}), 503
    n = int(request.args.get("n", 20))
    from recommendation_engine import get_top_products
    data = get_top_products(spark_state["pagerank"], spark_state["products"], n=n)
    return jsonify(data)


@app.route("/api/category-stats")
def api_category_stats():
    if spark_state["status"] != "ready":
        return jsonify({"error": "System not ready"}), 503
    from recommendation_engine import get_category_stats
    data = get_category_stats(spark_state["pagerank"], spark_state["products"])
    return jsonify(data)


@app.route("/api/products")
def api_products():
    if spark_state["status"] != "ready":
        return jsonify({"error": "System not ready"}), 503
    rows = spark_state["products"].collect()
    return jsonify([r.asDict() for r in rows])


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
