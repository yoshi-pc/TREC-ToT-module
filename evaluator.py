from typing import Union, Dict, List
from pathlib import Path
import math
import pandas as pd
from .corpus import Corpus
from .queries import Queries

class Evaluator():
    def __init__(self, path: Union[Path, str]):
        """
        Args:
            path (Union[Path, str]): the path to qrels.txt
        """
        self.q_rels: Dict[str, str] = {}
        # k: query id
        # v: annotated doc id
        with open(path, "r") as fp:
            for line in fp:
                line_sp = line.split()
                self.q_rels[str(line_sp[0])] = str(line_sp[2])
        self.max_length = 1000
        
    def evaluate(self, result: Dict[str, List[str]], debug: bool = False) -> Dict[str, float]:
        """
        Args:
            result (Dict[str, List[str]]):
                key: query id
                value: list of doc_ids which is generated by the system.
            debug (bool): if True, the evaluator will not truncate the result and use pure ranks as results.
        Returns:
            Dict[str, float]: score dictionary
        """
        eval: Dict[str, float] = {}
        for k, v in result.items():
            # k: query id
            # v: list of doc ids
            ranked = v
            if not debug: # truncate
                ranked = ranked[:self.max_length]
            if self.q_rels[k] in ranked:
                r = ranked.index(self.q_rels[k]) + 1
                if debug:
                    eval[k] = r
                else:
                    eval[k] = 1.0 / math.log2(r+1)
            else:
                eval[k] = 0.0
        return eval
    
    def success_at(self, result: Dict[str, List[str]], k: int = 10) -> Dict[str, float]:
        """
        Args:
            result (Dict[str, List[str]]):
                key: query id
                value: list of doc_ids which is generated by the system.
            k (int):
                success@k.
        Returns:
            Dict[str, float]: score dictionary
        """
        eval: Dict[str, float] = {}
        for q, v in result.items():
            # k: query id
            # v: list of doc ids
            ranked = v
            if self.q_rels[q] in ranked:
                r = ranked.index(self.q_rels[q]) + 1
                if r > k:
                    eval[q] = 0.0
                else:
                    eval[q] = 1.0
        return eval
    
    def evaluate_df(self, result: Dict[str, List[str]], q: Queries, c: Corpus, debug: bool = False) -> pd.DataFrame:
        query_id = []
        query_title = []
        answer_doc_id = []
        answer_title = []
        score_dcg = []
        score_rank = []
        for k, v in result.items():
            query_id.append(k)
            query_title.append(q.get_info(q.search_by_id(k), "title"))
            ranked = v
            if not debug:
                ranked = ranked[:self.max_length]
            if self.q_rels[k] in ranked:
                r = ranked.index(self.q_rels[k]) + 1
                score_dcg.append(1.0 / math.log2(r+1))
                score_rank.append(-1)
            else:
                score_dcg.append(-1)
                score_rank.append(-1)
            answer_doc_id.append(self.q_rels[k])
            answer_title.append(c.get_info(c.search_by_doc_id(self.q_rels[k]), "page_title"))
        return pd.DataFrame({
            "query_id": query_id,
            "query_title": query_title,
            "answer_doc_id": answer_doc_id,
            "answer_title": answer_title,
            "score_dcg": score_dcg,
            "score_rank": score_rank
        })

    def agg(self, result: Dict[str, List[str]]) -> float:
        ret = self.evaluate(result)
        l = list(ret.values())
        return sum(l) / len(l)
