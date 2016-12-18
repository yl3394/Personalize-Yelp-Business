from flask import Flask
from flask import render_template

import spark_scripts.analysis as analysis
import spark_scripts.yelp_lib as lib

app = Flask(__name__)
app.config['DEBUG'] = True


@app.route('/')
def hello_world():
    return 'Hello World!!! (Flask edition)'


@app.route('/business/get_name', defaults={'name': None})
@app.route('/business/get_name/<name>')
def get_business_names(name):
    return lib.get_business_names(name)


@app.route('/business/build_chart/<business_id>')
def build_chart(business_id):
    business_info = analysis.get_business_info(business_id)
    business_name = business_info.name.values[0]
    return render_template('yelp_restaurant.html', business_id=business_id, business_name=business_name)


@app.route('/analysis/review_count/<business_id>')
def analysis_review_count(business_id):
    return analysis.get_review_count_by_date(business_id)


@app.route('/analysis/review_avg/<business_id>')
def analysis_review_avg(business_id):
    return analysis.get_review_avg_by_date(business_id)


if __name__ == '__main__':
    app.run()
