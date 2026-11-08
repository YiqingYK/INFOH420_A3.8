import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from paper_processing import get_processed_filenames, process_and_store_papers
from paper_processing.database import list_papers, store_paper
from paper_processing.llm import extract_metadata, validate_metadata


METADATA = {"title": "Test paper", "summary": "Short summary",
            "keywords": ["testing"], "category": "Computer Science"}


class ProcessingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "data" / "papers.db"
        for name in ("a.pdf", "b.pdf"):
            (self.root / name).write_bytes(b"fixture; converter mocked")

    @patch("paper_processing.pipeline.pdf_to_markdown", return_value="# Paper")
    @patch("paper_processing.pipeline.extract_metadata", return_value=METADATA)
    def test_idempotency_and_batch_replay(self, llm, convert):
        result = process_and_store_papers(["a.pdf", "a.pdf"], self.root, self.db, "run1")
        self.assertEqual(result["processed_filenames"], ["a.pdf"])
        self.assertEqual(process_and_store_papers(
            ["a.pdf"], self.root, self.db, "run1"), result)
        self.assertEqual(process_and_store_papers(
            ["a.pdf"], self.root, self.db, "run2")["processed_filenames"], [])
        self.assertEqual(llm.call_count, 1)
        self.assertEqual(convert.call_count, 1)
        self.assertEqual(list_papers(self.db)[0]["keywords"], ["testing"])
        self.assertEqual(list_papers(self.db)[0]["markdown"], "# Paper")

    @patch("paper_processing.pipeline.pdf_to_markdown", return_value="# Paper")
    @patch("paper_processing.pipeline.extract_metadata")
    def test_partial_failure_recovers_prior_commits(self, llm, convert):
        llm.side_effect = [METADATA, ValueError("bad JSON")]
        with self.assertRaises(ValueError):
            process_and_store_papers(["a.pdf", "b.pdf"], self.root, self.db, "run")
        self.assertEqual(get_processed_filenames(self.db), {"a.pdf"})
        llm.side_effect = None
        llm.return_value = METADATA
        result = process_and_store_papers(["a.pdf", "b.pdf"], self.root, self.db, "run")
        self.assertEqual(result["processed_filenames"], ["a.pdf", "b.pdf"])
        self.assertEqual(llm.call_count, 3)

    @patch("paper_processing.pipeline.extract_metadata")
    def test_empty_and_invalid_paths(self, llm):
        self.assertEqual(get_processed_filenames(self.db), set())
        self.assertEqual(process_and_store_papers(
            [], self.root, self.db, "empty")["processed_filenames"], [])
        for name in ("../a.pdf", "a\\b.pdf", "foo.txt", "/a.pdf"):
            with self.assertRaises(ValueError):
                process_and_store_papers([name], self.root, self.db, "bad")
        llm.assert_not_called()

    @patch("paper_processing.llm.urlopen")
    def test_http_contract_and_json_retry(self, urlopen):
        responses = []
        for content in ("invalid JSON", json.dumps(METADATA)):
            response = MagicMock()
            response.__enter__.return_value.read.return_value = json.dumps(
                {"response": content}).encode()
            responses.append(response)
        urlopen.side_effect = responses
        self.assertEqual(extract_metadata("# Paper")["title"], "Test paper")
        self.assertEqual(urlopen.call_count, 2)
        request = urlopen.call_args.args[0]
        payload = json.loads(request.data)
        self.assertFalse(payload["stream"])
        self.assertEqual(payload["format"]["type"], "object")

    @patch("paper_processing.llm.urlopen")
    def test_second_invalid_response_fails(self, urlopen):
        urlopen.return_value.__enter__.return_value.read.return_value = b'{"response":"[]"}'
        with self.assertRaises(ValueError):
            extract_metadata("Paper")
        self.assertEqual(urlopen.call_count, 2)

    def test_validation_and_link_evidence(self):
        with self.assertRaises(ValueError):
            validate_metadata({**METADATA, "keywords": "wrong type"}, "")
        actual = "https://github.com/example/repo"
        metadata = validate_metadata({**METADATA,
            "code_urls": [actual, "https://github.com/invented/repo"],
            "dataset_urls": ["javascript:alert(1)"]}, actual)
        self.assertEqual(metadata["code_urls"], [actual])
        self.assertEqual(metadata["dataset_urls"], [])

    def test_resource_links_are_independent_of_host(self):
        code = ["https://gitlab.com/team/project", "https://university.edu/source.zip"]
        # A GitHub repository may host a dataset instead of implementation code.
        datasets = ["https://zenodo.org/records/123", "https://github.com/team/dataset"]
        metadata = validate_metadata({**METADATA, "code_urls": code + [code[0]],
                                      "dataset_urls": datasets}, "\n".join(code + datasets))
        self.assertEqual(metadata["code_urls"], code)
        self.assertEqual(metadata["dataset_urls"], datasets)
        store_paper(self.db, "a.pdf", metadata, "\n".join(code + datasets), "run")
        paper = list_papers(self.db)[0]
        self.assertEqual(paper["code_urls"], code)
        self.assertEqual(paper["dataset_urls"], datasets)

    @patch("paper_processing.pipeline.pdf_to_markdown", return_value="# Paper")
    @patch("paper_processing.pipeline.extract_metadata", return_value={"title": "Incomplete"})
    def test_invalid_metadata_is_never_committed(self, llm, convert):
        with self.assertRaises(ValueError):
            process_and_store_papers(["a.pdf"], self.root, self.db, "bad")
        self.assertEqual(get_processed_filenames(self.db), set())

    def test_database_errors_propagate(self):
        with self.assertRaises(sqlite3.OperationalError):
            get_processed_filenames(self.root)  # a directory cannot be opened as a DB

    def test_real_pdf_conversion(self):
        import pymupdf
        from paper_processing.pdf import pdf_to_markdown

        source = self.root / "real.pdf"
        with pymupdf.open() as doc:
            page = doc.new_page()
            page.insert_text((72, 72), "A research paper about workflow automation.")
            doc.save(source)
        original = source.read_bytes()
        self.assertIn("workflow automation", pdf_to_markdown(source))
        self.assertEqual(source.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
