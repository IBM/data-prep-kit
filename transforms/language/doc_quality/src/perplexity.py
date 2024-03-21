import logging


logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger("perplexity")

import os

import kenlm
import sentencepiece
from cc_net_prepro import cc_net_normalize


# Ensure following path to folder having pre-trained KenLM + SentencePiece (sp):
# PATH_TO_PRETRAINED_MODELS = "/dev/cos/bluepile-processing/lm_sp/"  # on a VM
# PATH_TO_PRETRAINED_MODELS = '~/BigData/04-FM/lm_sp/'  # on a MAC

PATH_TO_PRETRAINED_MODELS = os.path.join(os.path.expanduser("~/BigData/04-FM/lm_sp/"), "en.sp.model")
# "~/Desktop/GUF_hajar/fm-data-engineering/transforms/language/doc_quality/lm_sp/"  # local
TEST_MODE = 0


class SentencePieceModel:
    """
    Tokenize a given text into tokens/subwords and then join them together with whitespace between tokens.
    Examples:
        For `en` text:
            mmodel_path = os.path.join(os.path.expanduser('~/BigData/04-FM/lm_sp/'), "en.sp.model")
            spm = SentencePieceModel(model_path)
            out_text = spm.tokenize(text="Café élevàtor ôperàtor naïve Noël façade don't",
                                    strip_accent=True, # set to False for comparison
                                    lower_case=True,
                                    digit_2_zero=True,
                                    punct_level=1,
                                    language='en',
                                    verbose=True)
            >>>
                == input text: Café élevàtor ôperàtor naïve Noël façade don't
                == normalized text: Café élevàtor ôperàtor naïve Noël façade don't
                -> tokenized to 9 tokens: ['▁cafe', '▁elevator', '▁operator', '▁naive', '▁noel', '▁facade', '▁don', "'", 't']
                -> output text: ▁cafe ▁elevator ▁operator ▁naive ▁noel ▁facade ▁don ' t
            Compared to w/o Unicode normalization:
                -> tokenized to 15 tokens: ['▁cafe', '▁elevator', '▁operator', '▁na', 'ï', 've', '▁no', 'ë', 'l', '▁fa', 'ç', 'ade', '▁don', "'", 't']
                -> output text: ▁cafe ▁elevator ▁operator ▁na ï ve ▁no ë l ▁fa ç ade ▁don ' t
        For `ja` text (trained by IBM TRL team):
            model_path = os.path.join(os.path.expanduser('~/BigData/04-FM/lm_sp/'), "ja.sp.model")
            spm = SentencePieceModel(model_path)
            out_text = spm.tokenize(text='これは机です。あれは鉛筆です。',
                                    strip_accent=True,
                                    lower_case=False,
                                    digit_2_zero=True,
                                    punct_level=1,
                                    language='ja',
                                    verbose=True)
            >>>
                == input text: これは机です。あれは鉛筆です。
                == normalized text: これは机です。あれは鉛筆です。
                -> tokenized to 10 tokens: ['▁', 'これは', '机', 'です', '.', 'あれ', 'は', '鉛筆', 'です', '.']
                -> output text: ▁ これは 机 です . あれ は 鉛筆 です .
        For same text but using `ja_meta` (trained by Meta):
            model_path = os.path.join(os.path.expanduser('~/BigData/04-FM/lm_sp/'), "ja_meta.sp.model")
            spm = SentencePieceModel(model_path)
            out_text = spm.tokenize(text='これは机です。あれは鉛筆です。',
                                    strip_accent=False,
                                    lower_case=False,
                                    digit_2_zero=True,
                                    punct_level=1,
                                    language='ja',
                                    verbose=True)
            >>>
                == input text: これは机です。あれは鉛筆です。
                == normalized text: これは机です。あれは鉛筆です。
                -> tokenized to 12 tokens: ['▁', 'これは', '机', 'で', 'す', '.', 'あれ', 'は', '鉛筆', 'で', 'す', '.']
                -> output text: ▁ これは 机 で す . あれ は 鉛筆 で す .
    """

    def __init__(self, pre_trained_model: str):
        super().__init__()
        try:
            self.pre_trained_model = pre_trained_model
            self.sp = sentencepiece.SentencePieceProcessor()
            self.sp.load(self.pre_trained_model)
            logger.info(f"== PRE-TRAINED SENTENCE PIECE: {self.pre_trained_model}")
        except Exception as e:
            raise Exception(f"== Failed in loading {pre_trained_model} due to: {e}")

    def tokenize(
        self,
        text: str,
        strip_accent: bool,
        lower_case: bool,
        digit_2_zero: bool,
        punct_level: int,
        language: str,
        verbose=False,
    ) -> str:
        """
        Tokenize given text into tokens then join them with whitespace as separation into
        a single out_text to return.
        """

        # Normalize `text` prior to tokenizing it into tokens
        text_normalized = cc_net_normalize(
            text,
            strip_accent=strip_accent,
            lower_case=lower_case,
            digit_2_zero=digit_2_zero,
            punct_level=punct_level,
            language=language,
        )

        # Conduct tokenization
        tokenized = self.sp.encode_as_pieces(text_normalized)
        out_text = " ".join(tokenized)
        if verbose:
            print(f"==== sentence piece model: {self.pre_trained_model} ====\n")
            print(f"== input text: {text}")
            print(f"== normalized text: {text}")
            print(f"-> tokenized to {len(tokenized)} tokens: {tokenized}")
            print(f"-> output text: {out_text}")
        return out_text


class KenLMModel:
    """
    Load a pre-trained KenLM and its corresponding SentencePiece tokenizer based on a given language
    to compute perplexity of a given text. The given text can be `normalized` by cc_net_normalize().
    """

    def __init__(
        self,
        model_path: str,
        language: str = "en",
        strip_accent: bool = True,
        lower_case: bool = True,
        digit_2_zero: bool = True,
        punct_level: int = 1,
    ):
        logger.info(f"== PATH TO PRETRAINED KenLM and SENTENCEPIECE: {model_path}")
        logger.info(f"== LANGUAGE: {language}")
        logger.info(f"== STRIP ACCENT: {strip_accent}")
        logger.info(f"== CONVERT TO LOWER CASE: {lower_case}")
        logger.info(f"== CONVERT DIGITS TO ZERO: {digit_2_zero}")
        logger.info(f"== LEVEL OF REPLACING PUNCTUATION: {punct_level}")

        """Load pre-trained KenLM and sp: """
        try:
            self.kenlm_model = kenlm.Model(os.path.join(model_path, f"{language}.arpa.bin"))
            self.sp_tokenizer = SentencePieceModel(os.path.join(model_path, f"{language}.sp.model"))
        except Exception as e:
            raise Exception(f"== Failed in loading pre-trained KenLM/SentencePiece of `{language}` due to: {e}")
        self.strip_accent = strip_accent
        self.lower_case = lower_case
        self.digit_2_zero = digit_2_zero
        self.punct_level = punct_level
        self.language = language

    @classmethod
    def from_pretrained(
        cls,
        model_path: str,
        language: str = "en",
        strip_accent: bool = True,
        lower_case: bool = True,
        digit_2_zero: bool = True,
        punct_level: int = 1,
    ):
        return cls(
            model_path,
            language,
            strip_accent,
            lower_case,
            digit_2_zero,
            punct_level,
        )

    def get_perplexity_ver01(self, doc: str) -> float:
        """
        This version normalizes the entire `doc` and compute its pplx score as a whole in a single shot.
        It can make the pplx score arbitrarily small by simply adding multiple short sentences like ".\n"
        which might not be a desired case. Hence, breaking doc into lines installed in get_perplexity()
        would be better handling this case.
        """

        # normalization will remove line breaks, making entire doc as a single line.
        doc_tokenized = self.sp_tokenizer.tokenize(
            doc,
            strip_accent=self.strip_accent,
            lower_case=self.lower_case,
            digit_2_zero=self.digit_2_zero,
            punct_level=self.punct_level,
            language=self.language,
        )  # this may combine all lines/paragraphs into one (i.e., no line breaks)
        """
        Tokenize text after normalizing it.
        Reference to full pipeline:
            https://github.com/facebookresearch/cc_net/blob/bda555bd1cf1ee2e0b925363e62a61cd46c8b60d/cc_net/mine.py#L352
        """

        if TEST_MODE:
            print(f"\n== raw doc: {doc}")
            print(f"\n== doc_tokenized: {doc_tokenized}")

        doc_log_score = self.kenlm_model.score(doc_tokenized)
        doc_length = len(doc_tokenized.split()) + 1  # no. of tokens

        if doc_length > 0:
            pp_score = round(10.0 ** (-doc_log_score / doc_length), 1)
        else:
            pp_score = float("inf")

        return pp_score

    def get_perplexity(self, doc: str) -> float:
        """
        This version breaks `doc` into lines and compute score per line
        and aggregate them to a single score of the entire `doc`.
        Parameters
        ----------
        doc: a text document
        Returns
        -------
        perplexity score as non-negative float score. The smaller the score, the more natural
        the language used in `doc`.
        Reference:
            https://github.com/facebookresearch/cc_net/blob/bda555bd1cf1ee2e0b925363e62a61cd46c8b60d/cc_net/mine.py#L352
            (which may differ by normalizing doc first)
        """

        doc_log_score, doc_length = 0, 0
        for i, line in enumerate(doc.split("\n")):
            line_tokenized = self.sp_tokenizer.tokenize(
                line,
                strip_accent=self.strip_accent,
                lower_case=self.lower_case,
                digit_2_zero=self.digit_2_zero,
                punct_level=self.punct_level,
                language=self.language,
            )

            if len(line_tokenized) == 0:
                continue
            log_score = self.kenlm_model.score(line_tokenized)
            length = len(line_tokenized.split()) + 1  # no. of tokens
            doc_log_score += log_score
            doc_length += length
            if TEST_MODE:
                print(
                    f"\n== {i:3d} length: {length} log_score: {log_score:.1f}\n + Original line: {line}\n + line_tokenized: {line_tokenized}"
                )
        if doc_length > 0:
            pp_score = round(10.0 ** (-doc_log_score / doc_length), 1)
        else:
            pp_score = float("inf")
        if TEST_MODE:
            print(
                f"\n=== doc_log_score: {doc_log_score:.1f} doc_length: {doc_length} -doc_log_score/doc_length:{(-doc_log_score/doc_length):.1f} pp_score:{pp_score:.1f}"
            )
        return pp_score


if __name__ == "__main__":
    # ''' Only sp tokenizer'''
    model_path = os.path.join(os.path.expanduser("~/BigData/04-FM/lm_sp/"), "ja.sp.model")
    spm = SentencePieceModel(model_path)
    out_text = spm.tokenize(
        text="これは机です。あれは鉛筆です。",
        strip_accent=True,
        lower_case=False,
        digit_2_zero=True,
        punct_level=1,
        language="ja",
        verbose=True,
    )
    #
    model_path = os.path.join(os.path.expanduser("~/BigData/04-FM/lm_sp/"), "en.sp.model")
    spm = SentencePieceModel(model_path)
    out_text = spm.tokenize(
        text="Café élevàtor ôperàtor naïve Noël façade don't",
        strip_accent=True,  # set to False for comparison
        lower_case=True,
        digit_2_zero=True,
        punct_level=1,
        language="en",
        verbose=True,
    )

    """ Get PPL's score via KenLM + sp model:"""
    # following doc has PPL= 525.7 if strip_accent=True, and = 528.4 if strip_accent=False (perhaps due to mix of en and ja)
    doc = "MENU ホーム 店舗のご案内 スタッフ紹介 求人情報 写真集 赤・ピンク系 黄色・オレンジ系 青・紫系 白系・緑系 ミックス その他 お問合せ よくある質問 プライバシーポリシー ブログ ホームHome 店舗のご案内About スタッフ紹介Staff 求人情報 写真集Photo 赤・ピンク系 黄色・オレンジ系 青・紫系 白系・緑系 ミックス その他 お問合せContact よくある質問 プライバシーポリシー ブログBlog その他 HOME その他 ムーンダスト 2021年5月9日/ 最終更新日 : 2021年5月10日hananoaceその他 ムーンダスト 本日は、母の日という事もありたくさんのご来店＆ご注文いただきまして誠にありがとうございます♪ふくやまです(^▽^)/ 母の日と言えば赤やピンクのカーネーションが真っ先に頭に浮かぶと思うのですが… 花のエースではその他にもたくさんの色をご用意しております(*^^*) こんな色もあるの、ご存じですか？？ ↓ Moondust（ムーンダスト） 世界で唯一花弁に青い色素を持つカーネーションです。 上品な色合いと花もちの良さが高く評価されています。 花言葉は『永遠の幸福』 ほぅ…高貴な色ですねぇ…( *´艸｀) 珍しい色のカーネーションをお探しの方にはオススメです(*^-^*) 母の日のタイミングを逃した方も、まだまだ間に合いますよ。 お母さんへの感謝の気持ちをぜひお花で贈ってみてください。きっと喜んでいただけると思いますよ♪ 関連記事を表示 花巻空港⛄ 2022年1月25日 お祝いスタンド花 2022年1月21日 白オンリーの花束 2022年1月17日 毛越寺 2022年1月12日 PayPayキャンペーン1/10まで♪ 2022年1月4日 お供え用花束 2021年12月26日 お祝いアレンジメント 2021年12月22日 お供え用アレンジメント 2021年12月18日 おうちでピザ作り🍕 2021年12月14日 縁起の良いお正月花❁ 2021年12月10日 カテゴリー その他 タグ ふくやま その他 前の記事 母の日ラッシュ～☆ 2021年5月8日 その他 次の記事 続・母の日 2021年5月10日 最近の投稿 花巻空港⛄ 2022年1月25日 退職のお祝いアレンジメント 2022年1月24日 久々のラーメン🍜穴場✨ 2022年1月23日 お誕生会 2022年1月22日 お祝いスタンド花 2022年1月21日 古希のお祝い 2022年1月20日 春色アレンジメント🌼 2022年1月19日 春の鉢物入荷 2022年1月18日 白オンリーの花束 2022年1月17日 お誕生日用のお花 2022年1月16日 カテゴリー 入荷情報 イベント＆行事 お花について アレンジメント 花束 スタンド 鉢 スタッフプライベート日記 その他 アーカイブ 2022年1月 2021年12月 2021年11月 2021年10月 2021年9月 2021年8月 2021年7月 2021年6月 2021年5月 2021年4月 2021年3月 2021年2月 2021年1月 2020年12月 2020年11月 2020年10月 2020年9月 2020年8月 2020年7月 2020年6月 2020年5月 2020年4月 2020年3月 2020年2月 2020年1月 2019年12月 2019年11月 2019年10月 お問い合わせ お気軽にお問い合わせください HOME サイトマップ 石鳥谷店 TEL：(0198)45-4556 四日町店 TEL：(0198)23-1623 花巻店 TEL：(0198)24-0575 営業時間 8:30～18:30 Copyright © 花のエース All Rights Reserved. Powered by WordPress with Lightning Theme & VK All in One Expansion Unit by Vektor,Inc. technology."
    # doc = '2021年9月19日 15時35分 新型コロナ 国内感染者数 群馬県は、県内で新たに34人が新型コロナウイルスに感染していることが確認されたと19日、発表しました。 続きを読む 県内で感染が確認された人は、1万6339人となり、このうち168人が死亡しています。 社会ニュース一覧へ戻る'
    # doc = '参院静岡、山口両選挙区の補欠選挙が７日、告示された。岸田政権発足後初めての国政選挙で、２４日に投開票される。与野党は衆院選（１９日公示、３１日投開票）の前哨戦と位置付け、選挙戦に臨む。新型コロナウイルス対策が主な争点となる。 岸田文雄首相は午後、ＪＲ静岡駅前で自民党候補の応援演説を予定。立憲民主党の杉尾秀哉副幹事長、国民民主党の玉木雄一郎代表はＪＲ静岡駅前で推薦候補の出陣式に参加する。'
    model_path = os.path.join(os.path.expanduser("~/BigData/04-FM/lm_sp/"))
    klm = KenLMModel.from_pretrained(
        model_path=model_path, language="ja", strip_accent=False, lower_case=True, digit_2_zero=True, punct_level=1
    )

    print(f"== Above doc has perplexity: {klm.get_perplexity(doc)}")

    model_path = os.path.join(os.path.expanduser("~/BigData/04-FM/lm_sp/"))
    klm = KenLMModel.from_pretrained(
        model_path=model_path, language="en", strip_accent=True, lower_case=True, digit_2_zero=True, punct_level=1
    )

    print(f'== "This text has perplexity": {klm.get_perplexity("This text has perplexity")}')
    print(f'== "Text has perplexity this": {klm.get_perplexity("Text has perplexity this")}')
