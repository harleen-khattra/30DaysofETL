from flask import Flask, jsonify

app = Flask(__name__)

# Load customer data
@app.route('/customers', methods=['GET'])
def get_customers():
    data = [
        {"customer_id": 101, "customer_name": "Alice", "city": "New York", "segment": "Individual"},
        {"customer_id": 102, "customer_name": "Bob", "city": "Los Angeles", "segment": "Corporate"},
        {"customer_id": 103, "customer_name": "Charlie", "city": "Chicago", "segment": "Individual"},
        {"customer_id": 104, "customer_name": "Diana", "city": "Houston", "segment": "SME"}
    ]
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
