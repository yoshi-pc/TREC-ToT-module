from typing import Union, List
from pathlib import Path
import json
from .trec_jsonl_parser import TRECJsonlParser

class Corpus(TRECJsonlParser):
    def __init__(self, path: Union[Path, str], wikiid_to_imdbid :Union[Path, str], delimiter :str = ".", dtype: dict = {}) -> None:
        super().__init__(path, delimiter, dtype)
        with open(wikiid_to_imdbid, "r") as fp:
            self.__imdb_id_map = json.load(fp)
        if len(self.__imdb_id_map) == 0:
            raise ValueError("cannot load the IMDb id mapping.")
    
    def search_by_wikidata_id(self, id: str) -> int:
        if not id.startswith("Q"):
            raise ValueError("wrong wikidata_id format.")
        df = self.get_df()
        hit = df.query(f"wikidata_id == '{id}'").index.tolist()
        if len(hit) > 1:
            raise ValueError("duplicated id was given.")
        elif len(hit) == 0:
            raise ValueError("unknown wikidata id was given.")
        return hit[0]

    def get_wikidata_classes_length(self, index: int) -> int:
        return len(self.get_info(index, "wikidata_classes"))

    def get_sections_keys(self, index: int) -> list:
        return list(self.get_info(index, "sections").keys())
    
    def get_infoboxes_length(self, index: int) -> int:
        return len(self.get_info(index, "infoboxes"))
    
    def search_by_doc_id(self, id: Union[str, int]) -> int:
        df = self.get_df()
        # DataFrameの型によってキャスト
        if df.dtypes["doc_id"] == "O":
            id = f"'{str(id)}'"
        else:
            id = int(id)
        ret = df.query(f"doc_id=={id}").index.tolist()
        if len(ret) > 1 or len(ret) == 0:
            raise ValueError(f"id is not unique. (length: {len(ret)})")
        return ret[0]

    def get_imdb_id(self, index: int, url: bool = False) -> List[str]:
        ret = self.__imdb_id_map.get(self.get_info(index, "doc_id"), [])
        if url:
            return [f"https://www.imdb.com/title/{item}/" for item in ret]
        else:
            return ret
    
    @staticmethod
    def help() -> str:
        return """
        You can use following paths to pull the information using Corpus.get_info method e.g. Corpus.get_info(index_num, "#######").
        You can obtain the key's meaning from TREC's homepage. See also https://trec-tot.github.io/guidelines.

        page_title
        page_source
        wikidata_id
        wikidata_classes
        text
        sections.* <- the elements under sections depend on the raw wikidata page's structure.
        infoboxes.[id].* <- the elements under infoboxes depend on its type.
        doc_id
        """
