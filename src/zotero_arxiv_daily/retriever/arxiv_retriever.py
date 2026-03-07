from .base import BaseRetriever, register_retriever
import arxiv
from arxiv import Result as ArxivResult
from ..protocol import Paper
from ..utils import extract_markdown_from_pdf
from tempfile import TemporaryDirectory
from urllib.request import urlretrieve
import os
from datetime import datetime, timedelta, timezone
from loguru import logger


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@register_retriever("arxiv")
class ArxivRetriever(BaseRetriever):
    def __init__(self, config):
        super().__init__(config)
        if self.retriever_config.category is None:
            raise ValueError("category must be specified for arxiv.")

    def _build_query(self) -> str:
        return " OR ".join(f"cat:{c}" for c in self.retriever_config.category)

    def _retrieve_raw_papers(self) -> list[ArxivResult]:
        client = arxiv.Client(num_retries=10, delay_seconds=10)
        query = self._build_query()
        days_back = int(self.retriever_config.get("days_back", 7))
        if days_back <= 0:
            raise ValueError("source.arxiv.days_back must be a positive integer.")
        cutoff = _utc_now() - timedelta(days=days_back)

        search = arxiv.Search(
            query=query,
            max_results=1000,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending,
        )

        raw_papers = []
        for paper in client.results(search):
            published = paper.published
            if published.tzinfo is None:
                published = published.replace(tzinfo=timezone.utc)
            if published < cutoff:
                break
            raw_papers.append(paper)
            if self.config.executor.debug and len(raw_papers) >= 10:
                break

        return raw_papers

    def convert_to_paper(self, raw_paper:ArxivResult) -> Paper:
        title = raw_paper.title
        authors = [a.name for a in raw_paper.authors]
        abstract = raw_paper.summary
        pdf_url = raw_paper.pdf_url
        with TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "paper.pdf")
            urlretrieve(pdf_url, path)
            try:
                full_text = extract_markdown_from_pdf(path)
            except Exception as e:
                logger.warning(f"Failed to extract full text of {title}: {e}")
                full_text = None
        return Paper(
            source=self.name,
            title=title,
            authors=authors,
            abstract=abstract,
            url=raw_paper.entry_id,
            pdf_url=pdf_url,
            full_text=full_text
        )
