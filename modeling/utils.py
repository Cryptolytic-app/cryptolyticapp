"""
This file contains all of the functions used in modeling for
the cryptolytic project.
"""

import glob
import pickle
import os
import shutil
import pickle
import json
import itertools
from zipfile import ZipFile
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")
import ast

import pandas as pd
import numpy as np
import datetime as dt

from ta import add_all_ta_features

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_recall_fscore_support
from sklearn.metrics import precision_score, recall_score, classification_report, roc_auc_score
from sklearn.metrics import accuracy_score, precision_score, f1_score, recall_score
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import ParameterGrid

############################################################    
#                     Variables
############################################################
# ALL_FEATURES = ['close_exchange_1','base_volume_exchange_1', 
#                     'nan_ohlcv_exchange_1','volume_adi_exchange_1', 'volume_obv_exchange_1',
#                     'volume_cmf_exchange_1', 'volume_fi_exchange_1','volume_em_exchange_1', 
#                     'volume_vpt_exchange_1','volume_nvi_exchange_1', 'volatility_atr_exchange_1',
#                     'volatility_bbhi_exchange_1','volatility_bbli_exchange_1', 
#                     'volatility_kchi_exchange_1', 'volatility_kcli_exchange_1',
#                     'volatility_dchi_exchange_1','volatility_dcli_exchange_1',
#                     'trend_macd_signal_exchange_1', 'trend_macd_diff_exchange_1', 
#                     'trend_adx_exchange_1', 'trend_adx_pos_exchange_1', 
#                     'trend_adx_neg_exchange_1', 'trend_vortex_ind_pos_exchange_1', 
#                     'trend_vortex_ind_neg_exchange_1', 'trend_vortex_diff_exchange_1', 
#                     'trend_trix_exchange_1', 'trend_mass_index_exchange_1', 
#                     'trend_cci_exchange_1', 'trend_dpo_exchange_1', 'trend_kst_sig_exchange_1',
#                     'trend_kst_diff_exchange_1', 'trend_aroon_up_exchange_1',
#                     'trend_aroon_down_exchange_1', 'trend_aroon_ind_exchange_1',
#                     'momentum_rsi_exchange_1', 'momentum_mfi_exchange_1',
#                     'momentum_tsi_exchange_1', 'momentum_uo_exchange_1',
#                     'momentum_stoch_signal_exchange_1', 'momentum_wr_exchange_1', 
#                     'momentum_ao_exchange_1', 'others_dr_exchange_1', 'close_exchange_2',
#                     'base_volume_exchange_2', 'nan_ohlcv_exchange_2',
#                     'volume_adi_exchange_2', 'volume_obv_exchange_2',
#                     'volume_cmf_exchange_2', 'volume_fi_exchange_2',
#                     'volume_em_exchange_2', 'volume_vpt_exchange_2',
#                     'volume_nvi_exchange_2', 'volatility_atr_exchange_2',
#                     'volatility_bbhi_exchange_2', 'volatility_bbli_exchange_2',
#                     'volatility_kchi_exchange_2', 'volatility_kcli_exchange_2',
#                     'volatility_dchi_exchange_2', 'volatility_dcli_exchange_2',
#                     'trend_macd_signal_exchange_2',
#                     'trend_macd_diff_exchange_2', 'trend_adx_exchange_2',
#                     'trend_adx_pos_exchange_2', 'trend_adx_neg_exchange_2',
#                     'trend_vortex_ind_pos_exchange_2',
#                     'trend_vortex_ind_neg_exchange_2',
#                     'trend_vortex_diff_exchange_2', 'trend_trix_exchange_2',
#                     'trend_mass_index_exchange_2', 'trend_cci_exchange_2',
#                     'trend_dpo_exchange_2', 'trend_kst_sig_exchange_2',
#                     'trend_kst_diff_exchange_2', 'trend_aroon_up_exchange_2',
#                     'trend_aroon_down_exchange_2',
#                     'trend_aroon_ind_exchange_2',
#                     'momentum_rsi_exchange_2', 'momentum_mfi_exchange_2',
#                     'momentum_tsi_exchange_2', 'momentum_uo_exchange_2',
#                     'momentum_stoch_signal_exchange_2',
#                     'momentum_wr_exchange_2', 'momentum_ao_exchange_2',
#                     'others_dr_exchange_2', 'year', 'month', 'day',
#                     'higher_closing_price', 'pct_higher', 
#                     'arbitrage_opportunity', 'window_length']

############################################################    
#                 Print Statements
############################################################
line = '-------------'
sp = '      '

def tbl_stats_headings():
    """Prints the headings for the stats table"""
    print(sp*2, line*9, '\n', 
          sp*3, 'Accuracy Score', 
#           sp, 'True Positive Rate',
#           sp, 'False Postitive Rate', 
          sp, 'Precision',
          sp, 'Recall',
          sp, 'F1', '\n',
          sp*2, line*9, '\n', 
    )
    
def tbl_stats_row(test_accuracy, precision, recall, f1):
    """Prints the row of model stats after each param set fold"""
    print(
        sp*4, f'{test_accuracy:.4f}',     # accuracy
#         sp*3, f'{tpr:.4f}',           # roc auc
#         sp*3, f'{fpr:.4f}',      # p/r auc
        sp*2, f'{precision:.4f}',      # p/r auc
        sp*1, f'{recall:.4f}',      # p/r auc
        sp*1, f'{f1:.4f}',     # p/r auc
        sp*2, line*9
    )

def print_model_name(name, i, arb_data_paths):
    print(
    '\n\n', line*9, '\n\n', 
    f'Model {i+1}/{len(arb_data_paths)}: {name}', '\n', 
    line*9
    )

def print_model_params(i, params, pg_list):  
    print(
        sp*2, line*5, '\n', 
        sp*2, f'Model {i+1}/{len(pg_list)}', '\n',  
        sp*2, f'params={params if params else None}', '\n', 
        sp*2, line*5
    )
    
############################################################    
#              Calculating Mean % Profit
############################################################
# specifying arbitrage window length to target, in minutes
interval = 30

def get_higher_closing_price(df):
    """
    Returns the exchange with the higher closing price
    """
    # exchange 1 has higher closing price
    if (df['close_exchange_1'] - df['close_exchange_2']) > 0:
        return 1
    
    # exchange 2 has higher closing price
    elif (df['close_exchange_1'] - df['close_exchange_2']) < 0:
        return 2
    
    # closing prices are equivalent
    else:
        return 0

def get_close_shift(df, interval=interval):
    """
    Shifts the closing prices by the selected interval +
    10 mins.
    
    Returns a df with new features:
    - close_exchange_1_shift
    - close_exchange_2_shift
    """
    
    rows_to_shift = int(-1*(interval/5))
    
    df['close_exchange_1_shift'] = df['close_exchange_1'].shift(
        rows_to_shift - 2)
    
    df['close_exchange_2_shift'] = df['close_exchange_2'].shift(
        rows_to_shift - 2)
    
    return df

def get_profit(df):
    """
    Calculates the profit of an arbitrage trade.
    
    Returns df with new profit feature.
    """
    
    # if exchange 1 has the higher closing price
    if df['higher_closing_price'] == 1:
        
        # return how much money you would make if you bought 
        # on exchange 2, sold on exchange 1, and took account 
        # of 0.55% fees
        return (((df['close_exchange_1_shift'] / 
                 df['close_exchange_2'])-1)*100)-.55
    
    # if exchange 2 has the higher closing price
    elif df['higher_closing_price'] == 2:
        
        # return how much money you would make if you bought 
        # on exchange 1, sold on exchange 2, and took account 
        # of 0.55% fees
        return (((df['close_exchange_2_shift'] / 
                 df['close_exchange_1'])-1)*100)-.55
    
    # if the closing prices are the same
    else:
        return 0 # no arbitrage

    
    
def profit(X_test, y_preds):  
    # creating dataframe from test set to calculate profitability
    test_with_preds = X_test.copy()

    # add column with higher closing price
    test_with_preds['higher_closing_price'] = test_with_preds.apply(
            get_higher_closing_price, axis=1)

    # add column with shifted closing price
    test_with_preds = get_close_shift(test_with_preds)

    # adding column with predictions
    test_with_preds['pred'] = y_preds

    # adding column with profitability of predictions
    test_with_preds['pct_profit'] = test_with_preds.apply(
            get_profit, axis=1).shift(-2)

    # filtering out rows where no arbitrage is predicted
    test_with_preds = test_with_preds[test_with_preds['pred'] != 0]

    # calculating mean profit where arbitrage predicted...
    pct_profit_mean = round(test_with_preds['pct_profit'].mean(), 2)

    # calculating median profit where arbitrage predicted...
    pct_profit_median = round(test_with_preds['pct_profit'].median(), 2)
    
    return pct_profit_mean, pct_profit_median

############################################################    
#                     Parameters
############################################################
def create_pg(param_grid):
    """
    Selects the correct features and parameters
    for each model
    """
    
    if not param_grid:
        pg_list = [param_grid]
        print('1 no params')
    else:
        for key in param_grid:
            if isinstance(param_grid[key], list):
                print('2 iterable params')
                continue  
            else:
                pg_list = param_grid
                print('3 reg params')
                break
            pg_list = [ParameterGrid(param_grid)]

    print('pg_list', pg_list)
    return pg_list


############################################################    
#                Correct Arb Predictions
############################################################
def confusion_feat(y_test, y_preds):
    """
    
    """
    
    # labels for confusion matrix
    unique_y_test = y_test.unique().tolist()
    unique_y_preds = list(set(y_preds))
    labels = list(set(unique_y_test + unique_y_preds))
    labels.sort()
    columns = [f'Predicted {label}' for label in labels]
    index = [f'Actual {label}' for label in labels]

    # create confusion matrix
    conf_mat = pd.DataFrame(confusion_matrix(y_test, y_preds),
                             columns=columns, index=index)
    print(conf_mat, '\n')
    print(classification_report(y_test, y_preds, digits=3))

    if 'Predicted 1' in conf_mat.columns and 'Predicted -1' in conf_mat.columns:
        # % incorrect predictions for 0, 1, -1
        pct_wrong_0 = (conf_mat['Predicted 0'][0] + 
                       conf_mat['Predicted 0'][2])/conf_mat['Predicted 0'].sum()
        pct_wrong_1 = (conf_mat['Predicted 1'][0] + 
                       conf_mat['Predicted 1'][1])/conf_mat['Predicted 1'].sum()
        pct_wrong_neg1 = (conf_mat['Predicted -1'][1] + 
                           conf_mat['Predicted -1'][2])/conf_mat['Predicted -1'].sum()
        # total number correct arbitrage preds (-1)
        correct_arb_neg1 = conf_mat['Predicted -1'][0]
        # total number correct arbitrage preds (1)
        correct_arb_1 = conf_mat['Predicted 1'][2]
        # total number correct arbitrage preds (-1) + (1)
        correct_arb = correct_arb_neg1 + correct_arb_1
        # total number correct no arbitrage preds (0)
        correct_arb_0 = conf_mat['Predicted 0'][1]
    # confusion matrix has 0, 1 predictions
    elif 'Predicted 1' in conf_mat.columns:
        pct_wrong_0 = conf_mat['Predicted 0'][1] / conf_mat['Predicted 0'].sum()
        pct_wrong_1 = conf_mat['Predicted 1'][0] / conf_mat['Predicted 1'].sum()
        pct_wrong_neg1 = np.nan
        # total number correct arbitrage preds (-1)
        correct_arb_neg1 = 0
        # total number correct arbitrage preds (1)
        correct_arb_1 = conf_mat['Predicted 1'][1]
        # total number correct arbitrage preds (-1) + (1)
        correct_arb = correct_arb_neg1 + correct_arb_1
        # total number correct no arbitrage preds (0)
        correct_arb_0 = conf_mat['Predicted 0'][0]
    # confusion matrix has -1, 0 predictions
    elif 'Predicted -1' in conf_mat.columns:
        pct_wrong_0 = conf_mat['Predicted 0'][0] / conf_mat['Predicted 0'].sum()
        pct_wrong_1 = np.nan
        pct_wrong_neg1 = conf_mat['Predicted -1'][1] / conf_mat['Predicted -1'].sum()
        # total number correct arbitrage preds (-1)
        correct_arb_neg1 = conf_mat['Predicted -1'][0]
        # total number correct arbitrage preds (1)
        correct_arb_1 = 0
        # total number correct arbitrage preds (-1) + (1)
        correct_arb = correct_arb_neg1 + correct_arb_1
        # total number correct no arbitrage preds (0)
        correct_arb_0 = conf_mat['Predicted 0'][1]
    # confusion matrix has only 0
    else:
        pct_wrong_0 = 0
        pct_wrong_1 = 0
        pct_wrong_neg1 = 0
        correct_arb = 0
        correct_arb_neg1 = 0
        correct_arb_1 = 0
        correct_arb_0 = 0
    
    return (pct_wrong_0, pct_wrong_1, pct_wrong_neg1, correct_arb, 
            correct_arb_neg1, correct_arb_1, correct_arb_0)

############################################################    
#                   Model Naming
############################################################
def model_names(param_grid, params, csv_name, model_label):
    # define model name
    if param_grid:
        model_id = '_'.join([
            csv_name, 
            model_label,
            str(params['max_features']), 
            str(params['max_depth']), 
            str(params['n_estimators'])
        ])
    else:
        model_id = csv_name + '_' + model_label

    # define model filename to check if it exists
    model_path = f'models/{model_id}.pkl'
    
    return model_id, model_path


############################################################    
#                 Train/Test Split
############################################################
def ttsplit(df, features, target):
    
    ## tt split
    # remove 2 weeks from train datasets to create a  
    # two week gap between the data - prevents data leakage
    tt_split_row = round(len(df)*.82)
    tt_split_time = df['closing_time'][tt_split_row]
    cutoff_time = tt_split_time - dt.timedelta(days=14)

    # train and test subsets
    train = df[df['closing_time'] < cutoff_time]
    test = df[df['closing_time'] > tt_split_time]
    
    # X, y matrix
    X_train = train[features]
    X_test = test[features]
    y_train = train[target]
    y_test = test[target]

    # printing shapes to track progress
    print(sp*2, 'train and test shape: ', train.shape, test.shape)
    
    return X_train, X_test, y_train, y_test

############################################################    
#                 Evaluation Metrics
############################################################
def model_eval(X_test, y_test, y_preds, model_id, csv_name, model_label, params):
    
    pct_prof_mean, pct_prof_median = profit(X_test, y_preds)
    print(sp*2,'percent profit mean:', pct_prof_mean)
    print(sp*2, 'percent profit median:', pct_prof_median, '\n\n')
    
    accuracy = accuracy_score(y_test, y_preds)
    
    precision = precision_score(y_test, y_preds, average='weighted')

    f1 = f1_score(y_test, y_preds, average='weighted')

    recall = recall_score(y_test, y_preds, average='weighted')

    pct_wrong_0, pct_wrong_1, pct_wrong_neg1, correct_arb, correct_arb_neg1, correct_arb_1, correct_arb_0 = confusion_feat(y_test, y_preds)
    
    eval_dict = {
        'model_id': model_id, 
        'csv_name': csv_name, 
        'model_label': model_label,
        'params': params,  
        'accuracy': accuracy,
        'precision': precision, 
        'recall': recall, 
        'f1': f1,
        'pct_profit_mean': pct_prof_mean, 
        'pct_profit_median': pct_prof_median,
        'pct_wrong_0': pct_wrong_0, 
        'pct_wrong_1': pct_wrong_1, 
        'pct_wrong_neg1': pct_wrong_neg1, 
        'correct_arb': correct_arb, 
        'correct_arb_neg1': correct_arb_neg1, 
        'correct_arb_1': correct_arb_1, 
        'correct_arb_0': correct_arb_0
    }
    
    return eval_dict   

############################################################    
#                      Modeling
############################################################
def create_models(arb_data_paths, model_type, features, param_grid, filename, export_preds=False, export_model=False):
    """
    This function takes in a list of all the arbitrage data paths, 
    does train/test split, feature selection, trains models, 
    saves the pickle file, and prints performance stats for each model

    Predictions
    ___________
    
    Models predict whether arbitrage will in 10 mins from the 
    prediction time, and last for at least 30 mins:
    1: arbitrage from exchange 1 to exchange 2
    0: no arbitrage
    -1: arbitrage from exchange 2 to exchange 1
    
    Evaluation
    __________
    
    - Accuracy Score
    - Precision
    - Recall
    - F1 score
    - Mean Percent Profit
    - Median Percent Profit

    Parameters
    __________
    
    arb_data_paths: filepaths for all the datasets used in modeling
    model_type: scikit-learn model (LogisticRegression() or 
        RandomForestClassifier())
    features: the features for training or empty [] for all features
    param_grid: the params used for hyperparameter tuning or empty {} 
    """   
    
    base_model_name = str(model_type).split('(')[0]
    model_name_dict = {
        'LogisticRegression': 'lr',
        'RandomForestClassifier': 'rf'
    }
    
    model_label = model_name_dict[base_model_name]

    if param_grid:
        model_label = model_label + '_hyper'

    # this is part of a check put in the code to allow the function
    # to pick up where it previously left off in case of errors
    
    # create features and param grid
    pg_list = create_pg(param_grid)
    
    if not features:
        with open ('all_features.txt', 'rb') as fp:
            features = pickle.load(fp)
            print('read feat')
#         features = ALL_FEATURES
    # pick target
    target = 'target'
    
    file = Path(filename)
    columns = ['model_id', 'csv_name', 'model_label', 'params', 'accuracy', 'precision', 
               'recall', 'f1', 'pct_profit_mean', 'pct_profit_median', 'pct_wrong_0', 
               'pct_wrong_1', 'pct_wrong_neg1', 'correct_arb', 'correct_arb_neg1', 
               'correct_arb_1', 'correct_arb_0']
        


    if file.exists(): 
        mp_df = pd.read_csv(filename)
    else:
#         mp_df = pd.DataFrame()
        mp_df = pd.DataFrame(columns=columns)
        

    # iterate through the arbitrage csvs
    for i, file in enumerate(arb_data_paths):
        # define model name
        csv_name = file.split('/')[2].split('.')[0]
            
        # print status
        print_model_name(csv_name, i, arb_data_paths)

        # read csv
        df = pd.read_csv(file, index_col=0)

        # convert str closing_time to datetime
        df['closing_time'] = pd.to_datetime(df['closing_time'])

        # train/test split the dataframe
        X_train, X_test, y_train, y_test = ttsplit(df, features, target)

        if ((X_train.shape[0] > 1000) 
            and (X_test.shape[0] > 100) 
            and len(set(y_train)) > 1):


            # hyperparameter tuning
            for i, params in enumerate(pg_list): 

                # define the model name and path
                model_id, model_path = model_names(param_grid, params, csv_name, model_label)
#                 print(model_id)
                if mp_df[mp_df['model_id'] == model_id].empty:


                    # print status
                    print_model_params(i, params, pg_list)

                    model = model_type.set_params(**params)

                    # there was a weird error caused by two of the datasets which
                    # is why this try/except is needed to keep the function running
    #                         try:

                    # fit model
                    model = model.fit(X_train, y_train)

                    # make predictions
                    y_preds = model.predict(X_test)

                    # https://scikit-learn.org/stable/modules/generated/sklearn.metrics.roc_auc_score.html
                    # 'weighted':
                    #Calculate metrics for each label, and find their average weighted by support (the number of true instances for each label). 
                    #This alters ‘macro’ to account for label imbalance; it can result in an F-score that is not between precision and recall.

                    # evaluate model performance and return result in a dictionary  
                    eval_dict = model_eval(X_test, y_test, y_preds, model_id, csv_name, model_label, params)

                    # append dictionary to model performance DF
                    mp_df = mp_df.append(eval_dict, ignore_index=True)

                    print(f"Appended row: CSV-{csv_name} param, model id-{model_id}")

                    if export_model == True:  
                        #save model
                        pickle.dump(model, open(f'models/{model_id}.pkl', 'wb'))

                    if export_preds == True:

                        predictions = pd.DataFrame(columns=['y_test', 'y_preds'])

                        predictions['y_test'] = y_test.tolist()

                        predictions['y_preds'] = y_preds

                        predictions.to_csv(f'data/arb_preds_test_data/{model_id}.csv', index=False)

                else:
                    print(f"{sp*2}model id found:{model_id}")
            #                         except:
            #                             print(line*3 + '\n' + line + 'ERROR' + line + '\n' + line*3)
            #                             break # break out of for loop if there is an error with modeling

        # dataset is too small
        else:
            print(f'{sp*2}ERROR: dataset too small for {csv_name}')

            
        # export df, end of CSV cycle
        mp_df.to_csv(filename, index=False)

