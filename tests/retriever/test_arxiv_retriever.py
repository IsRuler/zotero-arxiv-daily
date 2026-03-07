from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import feedparser

import zotero_arxiv_daily.retriever.arxiv_retriever as arxiv_retriever
from zotero_arxiv_daily.retriever.arxiv_retriever import ArxivRetriever


def _mock_feedparser_with_new_ids(monkeypatch, count: int) -> None:
    entries = [
        feedparser.FeedParserDict(
            id=f"oai:arXiv.org:2501.000{i:02d}",
            arxiv_announce_type="new",
        )
        for i in range(count)
    ]
    feed = feedparser.FeedParserDict(
        feed=feedparser.FeedParserDict(title="ok"),
        entries=entries,
    )
    monkeypatch.setattr(feedparser, "parse", lambda _: feed)


def test_arxiv_retriever_filters_results_by_days_back(config, monkeypatch):
    fixed_now = datetime(2026, 3, 7, tzinfo=timezone.utc)
    config.source.arxiv.days_back = 7
    config.executor.debug = False

    _mock_feedparser_with_new_ids(monkeypatch, count=2)

    results = [
        SimpleNamespace(published=fixed_now - timedelta(days=1)),
        SimpleNamespace(published=fixed_now - timedelta(days=6)),
        SimpleNamespace(published=fixed_now - timedelta(days=8)),
    ]

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def results(self, search):
            return iter(results)

    search_kwargs = {}

    class FakeSearch:
        def __init__(self, **kwargs):
            search_kwargs.update(kwargs)

    monkeypatch.setattr(arxiv_retriever, "_utc_now", lambda: fixed_now, raising=False)
    monkeypatch.setattr(arxiv_retriever.arxiv, "Client", FakeClient)
    monkeypatch.setattr(arxiv_retriever.arxiv, "Search", FakeSearch)

    papers = ArxivRetriever(config)._retrieve_raw_papers()

    assert len(papers) == 2
    assert search_kwargs["query"] == "cat:cs.AI OR cat:cs.CV OR cat:cs.LG OR cat:cs.CL"


def test_arxiv_retriever_debug_limits_to_ten(config, monkeypatch):
    fixed_now = datetime(2026, 3, 7, tzinfo=timezone.utc)
    config.source.arxiv.days_back = 7
    config.executor.debug = True

    _mock_feedparser_with_new_ids(monkeypatch, count=20)

    results = [
        SimpleNamespace(published=fixed_now - timedelta(days=1))
        for _ in range(20)
    ]

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def results(self, search):
            return iter(results)

    class FakeSearch:
        def __init__(self, **kwargs):
            pass

    monkeypatch.setattr(arxiv_retriever, "_utc_now", lambda: fixed_now, raising=False)
    monkeypatch.setattr(arxiv_retriever.arxiv, "Client", FakeClient)
    monkeypatch.setattr(arxiv_retriever.arxiv, "Search", FakeSearch)

    papers = ArxivRetriever(config)._retrieve_raw_papers()

    assert len(papers) == 10
