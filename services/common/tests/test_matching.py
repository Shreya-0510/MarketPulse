from pathlib import Path

from marketpulse_common.config import load_config
from marketpulse_common.matching import EntityMatcher, matched_ids

M = EntityMatcher(load_config(Path(__file__).resolve().parents[3] / "config").entities)


def ids(t):
    return matched_ids(M.match(t))


def test_cashtag_and_ticker():
    assert ids("Loading up on $NVDA before earnings") == ["nvda"]
    assert ids("MSFT beats estimates") == ["msft"]


def test_company_name():
    assert ids("Infosys raises guidance") == ["infy"]


def test_ambiguous_apple_needs_context_and_exclusions():
    assert ids("I ate an apple today") == []
    assert ids("apple pie recipe, apple stock jokes") == []        # excluded phrase wins
    assert ids("Apple shares rose after the iPhone launch") == ["aapl"]


def test_amazon_rainforest_excluded():
    assert ids("Amazon rainforest stock of timber") == []
    assert ids("Amazon stock jumps on AWS growth") == ["amzn"]


def test_all_caps_tickers_are_case_sensitive():
    assert ids("the rilly tcs of life") == []
    assert ids("TCS shares fall, Nifty IT drags") == ["tcs"]


def test_word_boundaries():
    assert ids("MSFTX is not a match") == []
    assert ids("") == []


def test_multiple_entities_and_scores():
    ms = M.match("$AAPL and Microsoft rally")
    assert {m.entity_id: m.score for m in ms} == {"aapl": 1.0, "msft": 0.9}
