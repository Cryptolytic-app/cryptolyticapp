# Arbitrage Modeling Notebooks
1. [Arbitrage Data Processing](https://github.com/Cryptolytic-app/cryptolyticapp/blob/master/modeling/1_arbitrage_data_processing.ipynb)
    - Contains the code to generate the data that is used to create the arbitrage models
  
2. [Arbitrage Model Training](https://github.com/Cryptolytic-app/cryptolyticapp/blob/master/modeling/2_arbitrage_model_training.ipynb)
    - Contains the code to create the arbitrage models
  
3. [Arbitrage Model Evaluation](https://github.com/Cryptolytic-app/cryptolyticapp/blob/master/modeling/3_arbitrage_model_evaluation.ipynb)
    - Contains the code and analysis to select models with the best performance
    - Model performance visualizations can be found in this notebook or [here](https://github.com/Cryptolytic-app/cryptolyticapp/tree/master/modeling/assets/visualizations)


# Trade Recommender Modeling Notebooks
1. [Trade Recommender Models](https://github.com/Cryptolytic-app/cryptolyticapp/blob/master/modeling/4_trade_recommender_models.ipynb)
   - Contains the code to create the trade recommender models

2. [Trade Recommender Performance Visualization](https://github.com/Cryptolyticapp/cryptolyticapp/blob/master/modeling/5_tr_performance_visualization.ipynb)
    - Model performance visualizations can be found in this notebook or [here](https://github.com/Cryptolytic-app/cryptolyticapp/tree/master/modeling/assets/visualizations)

#### Directory Structure
```
├── cryptolytic/                        <-- Root directory   
│   ├── modeling/                       <-- Directory for modeling work
│   │      │
│   │      ├──assets/                   <-- Directory with png assets used in notebooks
│   │      │
│   │      ├──data/                     <-- Directory containing all data for project
│   │      │   ├─ arb_data/             <-- Directory for train data after merging + FE pt.2
│   │      │   │   └── *.csv
│   │      │   │
│   │      │   ├─ arb_preds_test_data/  <-- Directory for test data w/ predictions
│   │      │   │   └── *.csv 
│   │      │   │
│   │      │   ├─ raw_data/             <-- Directory for raw training data
│   │      │   │   └── *.csv
│   │      │   │
│   │      │   ├─ ta_data/              <-- Directory for csv files after FE pt.1 
│   │      │   │   └── *.csv
│   │      │   │
│   │      │   ├─ zip_raw_data/         <-- Directory containing zip files of raw data
│   │      │   │   └── *.zip
│   │      │   │
│   │      │   ├─ features.txt          <-- All feature sets used in modeling
│   │      │   │
│   │      │   ├─ model_perf.csv        <-- Data from training baseline models and tuning
│   │      │   │
│   │      │   ├─ top_model_perf.csv    <-- Data from retraining and exporting best models
│   │      │   │
│   │      │   ├─ train_data_paths.txt  <-- Paths to training data that's used in modeling
│   │      │
│   │      ├── models/                  <-- Directory for all pickle models
│   │      │      └── *.zip
│   │      │
│   │      ├─ 1_arbitrage_data_processing.ipynb  <-- NB for data processing and creating csv
│   │      │
│   │      ├─ 2_arbitrage_modeling.ipynb         <-- NB for baseline models and tuning
│   │      │
│   │      ├─ 3_arbitrage_model_evaluation.ipynb <-- NB for model selection, eval, and viz
│   │      │
│   │      ├─ 4_trade_recommender_models.ipynb   <-- NB for trade recommender models
│   │      │
│   │      ├─ 5_tr_performance_viz.ipynb         <-- NB for trade recommender viz
│   │      │
│   │      ├─ environment.yml                    <-- Contains project dependencies
│   │      │
│   │      ├─ utils.py                           <-- All the functions used in modeling
│   │      │
```