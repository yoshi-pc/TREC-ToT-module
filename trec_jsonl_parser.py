import pandas as pd
from pathlib import Path
from typing import Union

class TRECJsonlParser():
    # private __dataframe
    # private __delimiter

    def __init__(self, path: Union[str, Path], delimiter: str = ".", dtype: dict = {}) -> None:
        self.__dataframe = pd.read_json(path, orient = "records", lines = True, dtype = dtype)
        self.__delimiter = delimiter
    
    def __len__(self) -> int:
        return len(self.__dataframe)
    
    def get_by_index(self, index: int) -> pd.Series:
        if index >= len(self):
            raise IndexError(f"index out of range. ({index} < {len(self)})")
        return self.__dataframe.iloc[index]

    def get_info(self, index: int, info_path: str = ""):
        ret = self.get_by_index(index)
        if info_path == "":
            return ret
        path_into_df = info_path.lower().split(self.__delimiter)
        for layer in path_into_df:
            if isinstance(ret, list):
                if layer.isdecimal():
                    ret = ret[int(layer)]
                else:
                    raise KeyError(f"The index format is wrong. ({layer})")
            elif isinstance(ret, dict):
                if layer in ret.keys():
                    ret = ret[layer]
                else:
                    raise KeyError(f"The key is not found in the dict. ({layer})")
            elif isinstance(ret, pd.Series):
                if layer in ret.index:
                    ret = ret[layer]
                else:
                    raise IndexError(f"The index is not found in the Series. ({layer})")
            else:
                type_info = str(type(ret))
                raise TypeError(f"The process for {type_info} is not defined.")
        return ret
    
    def get_df(self) -> pd.DataFrame:
        return self.__dataframe

    def exec_query(self, q: str, copy: bool = False) -> pd.DataFrame:
        if copy:
            return self.get_df().copy().query(q)
        else:
            return self.get_df().query(q)

    @staticmethod
    def help() -> str:
        return "This is the super class. Please call from a child class."

    def print_help(self) -> None:
        print(self.help())
