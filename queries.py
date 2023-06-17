from typing import Union, List, Dict, Optional
from pathlib import Path
import pandas as pd
from .trec_jsonl_parser import TRECJsonlParser
from .corpus import Corpus

class Queries(TRECJsonlParser):
    def __init__(self, path: Union[str, Path], corpus: Corpus, delimiter: str = ".", dtype: dict = {}) -> None:
        super().__init__(path, delimiter, dtype)
        self.__corpus = corpus

    def retrieve_corpus(self, q_index: int) -> pd.Series:
        """Args:
            q_index (int): Query's index number.

        Returns:
            pd.Series: the retrieved Series data from corpus associated with the requested query.
        """
        wd_id = self.get_wiki(q_index)["wikidata_id"]
        c_index = self.__corpus.search_by_wikidata_id(wd_id)
        return self.__corpus.get_by_index(c_index)
    
    def retrieve_corpus_detail(self, q_index: int, path: str):
        """Args:
            q_index (int): Query's index number.
            path (str): The path into the information. This must be compatible with DataFrame's architecture.
        """
        wd_id = self.get_wiki(q_index)["wikidata_id"]
        c_index = self.__corpus.search_by_wikidata_id(wd_id)
        return self.__corpus.get_info(c_index, path)
    
    def get_sentence_annotations_length(self, index: int) -> int:
        df = self.get_info(index, "sentence_annotations")
        return len(df)
    
    def get_wiki(self, index: int) -> Dict[str, str]:
        elmnt = ["wikipedia_url", "wikipedia_id", "wikidata_id"]
        return {item: str(self.get_info(index, item)) for item in elmnt}
    
    def search_by_title(self, title: str, ambiguous: bool = False, strict_mode: bool = False) -> Union[List[int], int]:
        """Args:
            title (str): The title you want to search.
            ambiguous (bool, optional): If True, you can look for the title using both ignoring case mode and partial match mode. Defaults to False.
            strict_mode (bool, optional): If True, this method returns a single result. Defaults is False.

        Raises:
            IndexError: When strict_mode is True, this error means that the result is empty.

        Returns:
            Union[List[int], int]: If strict_mode is True, this method returns the single index of the result. If not, this returns the list of index of the result.
        """
        df = self.get_df()
        if ambiguous:
            hit = df.query(f"title.str.lower().str.contains('{title.lower()}')").index.tolist()
        else:
            hit = df[df["title"] == title].index.tolist()
        
        if strict_mode:
            if len(hit) > 0:
                hit = hit[0]
            else:
                raise IndexError("hit title is empty.")
        return hit

    def search_by_text(self, body: str) -> List[int]:
        """
        Args:
            body (str): The text you want to search. This method always looks for the text in ambiguous mode i.e., both ignoring case mode and partial match mode is on.

        Returns:
            List[int]: This method returns the list of index.
        """
        df = self.get_df()
        hit = df.query(f"text.str.lower().str.contains('{body.lower()}')").index.tolist()
        return hit

    def search_by_id(self, id: Union[int, str]) -> int:
        df = self.get_df()
        ret = df.query(f"id=={int(id)}").index.tolist()
        if len(ret) > 1 or len(ret) == 0:
            raise ValueError(f"id is not unique. (length: {len(ret)})")
        return ret[0]

    @staticmethod
    def help() -> str:
        return """
        You can use following paths to pull the information using Queries.get_info method e.g. Queries.get_info(index_num, "sentence_annotations.3.labels.opinion").
        You can obtain the key's meaning from TREC's homepage. See also https://trec-tot.github.io/guidelines.

        ```
        id
        url     # url of post
        domain  # this property might return nothing but the value "movie".
        title   # title of title
        text    # body of post
        wikipedia_id
        wikipedia_url
        wikidata_id
        imdb_url
        sentence_annotations.[id].id
        sentence_annotations.[id].text
        sentence_annotations.[id].labels.opinion
        sentence_annotations.[id].labels.emotion
        sentence_annotations.[id].labels.hedging
        sentence_annotations.[id].labels.social
        sentence_annotations.[id].labels.comparison_relative
        sentence_annotations.[id].labels.search
        sentence_annotations.[id].labels.movie.music_compare
        sentence_annotations.[id].labels.movie.genre_audience
        sentence_annotations.[id].labels.movie.production_camera_angle
        sentence_annotations.[id].labels.movie.timeframe_singular
        sentence_annotations.[id].labels.movie.origin_actor
        sentence_annotations.[id].labels.movie.object
        sentence_annotations.[id].labels.movie.location_specific
        sentence_annotations.[id].labels.movie.person_fictional
        sentence_annotations.[id].labels.movie.production_visual
        sentence_annotations.[id].labels.movie.scene
        sentence_annotations.[id].labels.movie.negation
        sentence_annotations.[id].labels.movie.production_audio
        sentence_annotations.[id].labels.movie.character
        sentence_annotations.[id].labels.movie.genre_traditional_tone
        sentence_annotations.[id].labels.movie.release_date
        sentence_annotations.[id].labels.movie.location_type
        sentence_annotations.[id].labels.movie.origin_language
        sentence_annotations.[id].labels.movie.origin_movie
        sentence_annotations.[id].labels.movie.peron_real
        sentence_annotations.[id].labels.movie.plot
        sentence_annotations.[id].labels.movie.quote
        sentence_annotations.[id].labels.movie.category
        sentence_annotations.[id].labels.movie.music_specific
        sentence_annotations.[id].labels.movie.timeframe_plural
        sentence_annotations.[id].labels.context.cross_media
        sentence_annotations.[id].labels.context.physical_medium
        sentence_annotations.[id].labels.context.situational_count
        sentence_annotations.[id].labels.context.physical_user_location
        sentence_annotations.[id].labels.context.situational_evidence
        sentence_annotations.[id].labels.context.situational_witness
        sentence_annotations.[id].labels.context.temporal
        ```

        The length of `sentence_annotations` (= the maximum [id]) can be obtained by using Queries.get_sentence_annotations_length(index)
        """