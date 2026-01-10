from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    # Echo back the instances
    return jsonify({
        "predictions": [{"echo": inst} for inst in data.get('instances', [])],
        "deployedModelId": "echo-model"
    })

@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
