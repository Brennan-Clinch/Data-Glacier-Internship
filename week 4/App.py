#!/usr/bin/env python
# coding: utf-8

# In[7]:


from flask import Flask, request, render_template, jsonify
import numpy as np
import pickle
import pandas as pd

app = Flask(__name__,template_folder = 'templates')
model = pickle.load(open('Model/Treemodel.pickle','rb'))

@app.route('/', methods=['GET','POST'])
def home():
    return render_template('index.html')
@app.route('/predict',methods =['POST'])
def predict():
    '''
    For rendering results on html GUI
    '''
    float_features = [float(x) for x in request.form.values()]
    final_features = [np.array(float_features)]
    prediction = model.predict(final_features)
    output = prediction[0]
    return render_template('index.html', prediction_text = 'Class should be {}'.format(output))
if __name__ == "__main__":
    app.run(port = 5000, debug=True)


# In[ ]:




