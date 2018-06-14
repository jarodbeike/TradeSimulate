from flask import Flask
import engine

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    engine.main()
    app.run()
