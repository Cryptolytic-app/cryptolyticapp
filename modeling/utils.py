def create_models(arb_data_paths, model_type, features, param_grid):
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
    target = 'target'
    base_model_name = str(model_type).split('(')[0]
    model_name_dict = {
        'LogisticRegression':'lr',
        'RandomForestClassifier':'rf'
    }
    
    # this is part of a check put in the code to allow the function
    # to pick up where it previously left off in case of errors
    model_paths = glob.glob('models2/*.pkl')
    
    # iterate through the arbitrage csvs
    for i, file in enumerate(arb_data_paths):
        
        # read csv
        df = pd.read_csv(file, index_col=0)
        
        # convert str closing_time to datetime
        df['closing_time'] = pd.to_datetime(df['closing_time'])
        
        # print status
        name = file.split('/')[2].split('.')[0]
        print_model_name(name, i, arb_data_paths)
        
        # this makes the function dynamic for whether you want
        # to select features/hyperparameters or not
        features, pg_list = feat_and_params(df, features, param_grid)

        # hyperparameter tuning
        for i, params in enumerate(pg_list): 
            # define model name
            if param_grid:
                model_name = '_'.join([
                    name, 
                    str(params['max_features']), 
                    str(params['max_depth']), 
                    str(params['n_estimators'])
                ])
            else:
                model_name = name + '_' + model_name_dict[base_model_name]

            # define model filename to check if it exists
            model_path = f'models/{model_name}.pkl'

            # if the model does not exist
            if model_path not in model_paths:
                
                # print status
                print_model_params(i, params, pg_list)

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
  
                # filter out datasets that are too small
                if ((X_train.shape[0] > 1000) 
                    and (X_test.shape[0] > 100) 
                    and len(set(y_train)) > 1):

                    model = model_type.set_params(**params)

                    # there was a weird error caused by two of the datasets which
                    # is why this try/except is needed to keep the function running
#                         try:

                    # fit model
                    model = model.fit(X_train, y_train)

                    # make predictions
                    y_preds = model.predict(X_test)

                    pct_prof_mean, pct_prof_median = profit(X_test, y_preds)
                    print(sp*2,'percent profit mean:', pct_prof_mean)
                    print(sp*2, 'percent profit median:', pct_prof_median, '\n\n')

                    # classification report
                    print(classification_report(y_test, y_preds))

                    # save model
                    pickle.dump(model, open(f'models/{model_name}.pkl', 'wb'))

#                         except:
#                             print(line*3 + '\n' + line + 'ERROR' + line + '\n' + line*3)
#                             break # break out of for loop if there is an error with modeling

                # dataset is too small
                else:
                    print(f'{sp*2}ERROR: dataset too small for {name}')

            # the model exists
            else:
                print(f'{sp*2}{model_path} already exists.')