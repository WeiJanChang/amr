from pathlib import Path  # pathlib: module, Path: class. Checking if a path exist
from typing import Optional, List, Dict, Tuple  # typing: support for type hint
import pandas as pd
import pandas as pd
from pathlib import Path
from typing import Union
def load_json(p: Union[Path, str]) -> pd.DataFrame:
    """
    load json file

    :param p: json path or containing folder
    :return:
        pd.DataFrame
    """
    if isinstance(p, str):  # if the variable p is an instance of the str class
        p = Path(p)  # if yes, creates a new object of 'Path' class and assigns it to the variable 'p'

    if 'json' in p.name:
        return pd.read_json(p, encoding='utf-8')  # If the 'json in the p.name--> read this json file. If the string "json"
        # is not in the "name" attribute, this block of code will not execute and the function will return nothing
        # or continue with the next step of code.

    else:
        f = list(p.glob('*.json'))  # To check if there is any json file present in the path p or not by using glob method
        # and return a list of all the json files stored in 'p'
        if len(f) == 0:
            raise FileNotFoundError(f'no json file under the {p}')
        elif len(f) == 1:
            return pd.read_json(f[0], encoding='utf-8')
        else:
            raise RuntimeError(f'multiple json files under the {p}')




df = load_json('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data')

print(df)