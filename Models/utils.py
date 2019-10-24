def create_tables(credentials, exchanges):
    """ Connects to a database and creates unique tables for each
        cryptocurrency trading pair/exchange combination"""

    # create connection
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'], database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'], password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])

    # create cursor
    cur = conn.cursor()

    # create a list of table names
    table_list = []
    for exchange in exchanges:
        for trading_pair in exchanges[exchange]:
            for i in ['300', '3600']:
                table = exchange + '_' + trading_pair + '_' + i
                table_list.append(table)

    # create each table in table_list
    for table in table_list:
         cur.execute('''
                    CREATE TABLE {table}
                    (
                    time integer,
                    open float,
                    high float,
                    low float,
                    close float,
                    volume float
                    );
                    '''.format(table_name=table))

    # commit and close
    conn.commit()
    print("Table created Successfully!")
    conn.close()


def change_ohlcv_time(df, period):
    """ Changes the time period on cryptocurrency ohlcv data.
        Period is a string denoted by 'time_in_minutesT'(ex: '1T', '5T', '60T')."""

    # Set date as the index. This is needed for the function to run
    df = df.set_index(['date'])

    # Aggregation function
    ohlc_dict = {                                                                                                             
    'open':'first',                                                                                                    
    'high':'max',                                                                                                       
    'low':'min',                                                                                                        
    'close': 'last',                                                                                                    
    'base_volume': 'sum'
    }

    # Apply resampling
    df = df.resample(period, how=ohlc_dict, closed='left', label='left')
    
    return df

def fill_nan(df):
  
    """Iterates through a dataframe and fills NaNs with appropriate open, high, low, close values."""

    # Forward fill close column.
    df['close'] = df['close'].ffill()

    # Backward fill the open, high, low rows with the close value.
    df = df.bfill(axis=1)

    return df

def feature_engineering(df):
    """Takes in a dataframe of 5 minute cryptocurrency trading data
        and returns a new dataframe with 1 hour data and new technical analysis features:
    """
    
    # Add a datetime column to df
    df['date'] = pd.to_datetime(df['closing_time'], unit='s')
     
    # Convert df to one hour candles
    period = '60T'
    df = change_ohlcv_time(df, period)
    
    # Add feature to indicate user inactivity.
    df['nan_ohlc'] = df['close'].apply(lambda x: 1 if pd.isnull(x) else 0)
    
    # Fill in missing values using fill function.
    df = fill_nan(df)
    
    # Reset index.
    df = df.reset_index()
    
    # Create additional date features.
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    
    # Add technical analysis features.
    df = add_all_ta_features(df, "open", "high", "low", "close", "base_volume")
      
    # Replace infinite values with NaNs.
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # Drop any features whose mean of missing values is greater than 20%.
    df = df[df.columns[df.isnull().mean() < .2]]
    
    # Replace remaining NaN values with the mean of each respective column and reset index.
    df = df.apply(lambda x: x.fillna(x.mean()),axis=0)
    
    # Create a feature for close price difference 
    df['close_diff'] = (df['close'] - df['close'].shift(1))/df['close'].shift(1)    
    
    # Function to create target
    def price_increase(x):
        if (x-(.70/100)) > 0:
            return True
        else:
            return False
    
    # Create target
    target = df['close_diff'].apply(price_increase)
    
    # To make the prediction before it happens, put target on the next observation
    target = target[1:].values
    df = df[:-1]
    
    # Create target column
    df['target'] = target
    
    # Remove first row of dataframe bc it has a null target
    df = df[1:]
    
    # Pick features
    features = ['open', 'high', 'low', 'close', 'base_volume', 'nan_ohlc', 
                'year', 'month', 'day', 'volume_adi', 'volume_obv', 'volume_cmf', 
                'volume_fi', 'volume_em', 'volume_vpt', 'volume_nvi', 'volatility_atr', 
                'volatility_bbh', 'volatility_bbl', 'volatility_bbm', 'volatility_bbhi', 
                'volatility_bbli', 'volatility_kcc', 'volatility_kch', 'volatility_kcl', 
                'volatility_kchi', 'volatility_kcli', 'volatility_dch', 'volatility_dcl', 
                'volatility_dchi', 'volatility_dcli', 'trend_macd', 'trend_macd_signal', 
                'trend_macd_diff', 'trend_ema_fast', 'trend_ema_slow', 
                'trend_adx_pos', 'trend_adx_neg', 'trend_vortex_ind_pos', 
                'trend_vortex_ind_neg', 'trend_vortex_diff', 'trend_trix', 
                'trend_mass_index', 'trend_cci', 'trend_dpo', 'trend_kst', 
                'trend_kst_sig', 'trend_kst_diff', 'trend_ichimoku_a', 
                'trend_ichimoku_b', 'trend_visual_ichimoku_a', 'trend_visual_ichimoku_b', 
                'trend_aroon_up', 'trend_aroon_down', 'trend_aroon_ind', 'momentum_rsi', 
                'momentum_mfi', 'momentum_tsi', 'momentum_uo', 'momentum_stoch', 
                'momentum_stoch_signal', 'momentum_wr', 'momentum_ao',  
                'others_dr', 'others_dlr', 'others_cr', 'close_diff', 'date', 'target']
    df = df[features]
    
    return df


def modeling_pipeline(csv_filenames):
    """Takes csv file paths of data for modeling, performs feature engineering,
        train/test split, creates a model, reports train/test score, and saves
        a pickle file of the model in a directory called /pickles."""
    
    for file in csv_filenames:
        
        # define model name 
        name = file.split('/')[1][:-9]
        print(name)
        
        # read csv
        df = pd.read_csv(file, index_col=0)
        
        # engineer features
        df = feature_engineering(df)
        
        # train test split
        train = df[df['date'] < '2018-09-30 23:00:00'] # cutoff sept 30 2018
        test = df[df['date'] > '2019-01-31 23:00:00'] # cutoff jan 31 2019
        print('train and test shape ({model}):'.format(model=name), train.shape, test.shape)
        
        # features and target
        features = df.drop(columns=['target', 'date']).columns.tolist()
        target = 'target'
        print(features)

        # define X, y vectors
        X_train = train[features]
        X_test = test[features]
        y_train = train[target]
        y_test = test[target]
        
        # instantiate model
        model = RandomForestClassifier(max_depth=50, n_estimators=100, n_jobs=-1, random_state=42)
        
        # fit model
        if X_train.shape[0] > 1000:
            model.fit(X_train, y_train)
            print('model fitted')

            # train accuracy
            train_score = model.score(X_train, y_train)
            print('train accuracy:', train_score)

            # make predictions
            y_preds = model.predict(X_test)
            print('predictions made')

            # test accuracy
            score = accuracy_score(y_test, y_preds)
            print('test accuracy:', score)

            # return model pkl
            pickle.dump(model, open('pickles/{model}.pkl'.format(model=name), 'wb'))
            print('{model} pickle saved!'.format(model=model))
            
        else:
            print('{model} does not have enough data!'.format(model=name))
            

def performance(X_test, y_preds):
    """ Takes in a test dataset and a model's predictions, calculates and returns
        the profit or loss """
    
    max_profit = 9000
    fee_rate = 0.0001

    df_preds = X_test
    df_preds['y_preds'] = y_preds
    
    
    df_preds['binary_y_preds'] = df_preds['y_preds'].shift(1).apply(lambda x: 1 if x == True else 0)
    performance = ((10000 * df_preds['binary_y_preds']*df_preds['close_diff']).sum())
    
    df_preds['preds_count'] = df_preds['binary_y_preds'].cumsum()
    increase_count = df_preds['preds_count'].diff(1)
    df_preds['increase_count'] = increase_count
    df_preds['trade_trig'] = df_preds['increase_count'].diff(1)
    number_of_entries = (df_preds.trade_trig.values==1).sum()
    
    adj_performance = performance - (number_of_entries * 2 * fee_rate * 10000)
    
    if performance > max_profit:
        perf_type = 'adjusted'
        return adj_performance, perf_type
    
    else:
        perf_type = 'regular'
        return performance, perf_type
    

def profit_and_loss(model_paths, csv_paths):
    """ Takes a list of pickled model paths and csv_paths and prints
        a profit and loss statement. Returns a list of the profits and
        losses."""
    
    pnl_list = []
    line = '------------------------------'
    space1 = '                             '
    space2 = '                            '
    space3 = '                         '
    for model_path in sorted(model_paths):
        # define model name
        model_name = model_path.split('/')[1][:-4]
        
        for csv_path in csv_paths:
            # define which dataset
            csv_name = csv_path.split('/')[1][:-9]
            
            # find the right match for model and data
            if csv_name == model_name:
                print(line, model_name.upper(), line)
                # create df
                df = pd.read_csv(path, index_col=0)
                
                # engineer features
                df = feature_engineering(df)

                # create test set
                test = df[df['date'] > '2019-01-31 23:00:00'] # cutoff jan 31 2019
                print('{space} test data rows:'.format(space=space1), test.shape[0])
                print('{space} test data features:'.format(space=space2), test.shape[1])
                
                # features and target
                features = df.drop(columns=['target', 'date']).columns.tolist()
                target = 'target'

                # define X, y vectors
                X_test = test[features]
                y_test = test[target]
                
                # load model
                loaded_model = pickle.load(open(model_path, 'rb'))
                
                # make predictions
                y_preds = loaded_model.predict(X_test)
                
                # calculate performance
                pnl, perf_type = performance(X_test, y_preds)
                print(space3 + 'The model\'s profit is ${pnl}'.format(perf_type=perf_type, pnl=round(pnl,2)) + '\n\n')
                pnl_list.append('{model_name} performance:  '.format(model_name=model_name) + str(round(pnl, 2)))

    return pnl_list