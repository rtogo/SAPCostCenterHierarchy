import os
import ks13

if __name__ == '__main__':
    data_sources = {
        'ks13': {'pasta': os.path.dirname(__file__), 'arquivo': 'ks13.txt'},
        'ksh3': {'pasta': os.path.dirname(__file__), 'arquivo': 'ksh3.txt'}
    }

    df = ks13.parse(data_sources)
    print(df)
