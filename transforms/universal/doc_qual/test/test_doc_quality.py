import pyarrow as pa
from transforms.universal.doc_qual.doc_Gopher_statistics import (
    compute_average_japanese_sentence_length,
    compute_bullet_point_ellipsis_alphabet_word_ratio,
    compute_word_statistics,
    contains_common_English_words,
    find_first_japanese_alphabet_position,
)
from transforms.universal.doc_qual.perplexity import KenLMModel

def test_gopher_stats():
    document_ids = pa.array([1001])
    documents = pa.array(
        [
            "タッチダウンオフィスは、LANと電源を用意して他のオフィスからやってきた利用者が作業できる環境を整えた場所。 "
            "生産性の向上を目的に通常オフィスの内部に設けられる。 東京駅前に企業と契約して社員にタッチダウンオフィスを提供す"
            "るサービスを展開するデスカットのような施設もある。 余ったスペースにLANと電源を用意してつくった出張者席。 "
            "社内のバッファスペースとして位置付けていたが、オフィス面積の削減で営業社員に固定席を設定しないフリーアドレス制を導入したために、"
            "全部の席がタッチダウン化してしまう事もある。 たった2時間の会議の為に出張したものの、気になるのでメールだけでもずっとみていたい、"
            "という時には便利である。"
        ]
    )
    table = pa.Table.from_arrays([document_ids, documents], names=["document_id", "contents"])
    ft_lang = 'ja'

    for text in table['contents'].to_pylist():
        (total_words, mean_word_len, symbol_to_word_ratio) = compute_word_statistics(text)
        assert 0.0 == symbol_to_word_ratio
        assert False == contains_common_English_words(text, ft_lang)
        assert 51 == compute_average_japanese_sentence_length(text)
        assert 0 == find_first_japanese_alphabet_position(text)
        (bullet_point_ratio, ellipsis_line_ratio, alphabet_word_ratio) = compute_bullet_point_ellipsis_alphabet_word_ratio(text)
        assert 0.0 == bullet_point_ratio
        assert 0.0 == ellipsis_line_ratio
        assert 1.0 == alphabet_word_ratio


def test_kenlm_score():
    document_ids = pa.array([1001])
    documents = pa.array(
        [
            "タッチダウンオフィスは、LANと電源を用意して他のオフィスからやってきた利用者が作業できる環境を整えた場所。 "
            "生産性の向上を目的に通常オフィスの内部に設けられる。 東京駅前に企業と契約して社員にタッチダウンオフィスを提供す"
            "るサービスを展開するデスカットのような施設もある。 余ったスペースにLANと電源を用意してつくった出張者席。 "
            "社内のバッファスペースとして位置付けていたが、オフィス面積の削減で営業社員に固定席を設定しないフリーアドレス制を導入したために、"
            "全部の席がタッチダウン化してしまう事もある。 たった2時間の会議の為に出張したものの、気になるのでメールだけでもずっとみていたい、"
            "という時には便利である。"
        ]
    )
    table = pa.Table.from_arrays([document_ids, documents], names=["document_id", "contents"])
    ft_lang = 'ja'
    strip_accent = True
    klm = KenLMModel.from_pretrained(model_path='/Docfilter/lm_sp/', language=ft_lang, strip_accent=strip_accent)
    for text in table['contents'].to_pylist():
        assert 177.9 == klm.get_perplexity(text)

if __name__ == "__main__":
    test_kenlm_score()
    test_gopher_stats()