#!/usr/bin/env python3
# !pip install pandas
import sys, time
import pandas as pd

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: new-stock-price-feeder.py <stock data file path>", file=sys.stderr)
        sys.exit(-1)

    # print ('Argv', sys.argv, flush=True)

    # set up dataframe of stock data    
    file_path = sys.argv[1]
    combined = pd.read_csv(file_path)

    aapl_df = combined[combined['Symbol'] == "AAPL"].set_index('datetime')
    msft_df = combined[combined['Symbol'] == "MSFT"].set_index('datetime')

    aapl_df.sort_index(axis=0, inplace=True)
    msft_df.sort_index(axis=0, inplace=True)

    max_rows = msft_df.shape[0]
    
    # Walk down MSFT stock dataframe. (This had more rows than AAPL stock data).
    # Print MSFT and AAPL stock data for datetimes that exist in both
    time.sleep(10) # give time to set up the listener
    for i in range(max_rows):
        index = msft_df.iloc[i].name
        
        if index in aapl_df.index:
            msft_price = msft_df.loc[index]['open']
            aapl_price = aapl_df.loc[index]['open']
            time.sleep(0.5)
            print('%10s\t%.4f\t%4s' % (index, msft_price, 'MSFT'), flush=True)
            print('%10s\t%.4f\t%4s' % (index, aapl_price, 'AAPL'), flush=True)
