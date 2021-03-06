
from flask import current_app, send_file, send_from_directory
import sys 
import os
import plotly
import plotly.express as px
import json
import plotly.graph_objs as go
import pandas as pd
import numpy as np

from flask import Flask, render_template,request 
from datetime import datetime
import pandas as pd
from werkzeug.utils import secure_filename 
app = Flask(__name__)


import seaborn as se
import matplotlib.pyplot as plt
plt.switch_backend('agg')
import numpy as np 


k=0
# read the data from the csv file in static folder
df = pd.read_csv('static/ressources.csv')
## to get current year
@app.context_processor
def inject_now():
    return {'now': datetime.utcnow()}

@app.route("/")
def home():
    

    return render_template("index.html", column_names=df.columns.values, row_data=list(df.values.tolist()), zip=zip)
        



@app.route("/About")
def About():
    return render_template("About.html")


if __name__ == "__main__":
    app.run(debug=True)