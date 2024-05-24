import polars as pl
import pandas as pd

def pick_qualitative():
    """ """


    # load data
    df = pl.read_parquet("data/raw.parquet")

    # load target pmid
    pmid_target_list = list(pd.read_csv('data/pmid_qualitative.txt')['PMID'].astype(str))

    # filter
    target_qual = ["Qualitative", "qualitative"]
    target_stud = ["Study", "Studies", "Review", "study", "studies", "review", "focus group", "Focus group"]
    target_sjog = ['Sjogren', 'sjogren', 'Sjögren', 'sjögren', 'pSS', 'pss', 'SjS', 'sjs']
    df = df.filter(
        (pl.col('TITLE').str.contains_any(target_qual) | pl.col('ABSTRACT').str.contains_any(target_qual)) &
        (pl.col('TITLE').str.contains_any(target_stud) | pl.col('ABSTRACT').str.contains_any(target_stud)) &
        (pl.col('TITLE').str.contains_any(target_sjog) | pl.col('ABSTRACT').str.contains_any(target_sjog)) &
        (pl.col('PMID').is_in(pmid_target_list))
    )

    # save
    print(df)
    df.write_parquet('data/processed.parquet')
    












if __name__ == "__main__":

    pick_qualitative()
