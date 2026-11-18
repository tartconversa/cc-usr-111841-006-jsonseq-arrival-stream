import json
from pathlib import Path
import tempfile
import threading
import unittest
from acquire.decode import decode
from acquire.origin import make_server
from acquire.service import collect_one
from acquire.store import Store
from acquire.transport import fetch

class AcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.database=str(Path(self.temporary.name)/'data.sqlite3')
        self.scenario=json.loads(Path('fixtures/scenario.json').read_text(encoding='utf-8'))
        self.server=make_server(self.scenario)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
        self.url=f'http://127.0.0.1:{self.server.server_port}'+self.scenario['entry_path']
    def tearDown(self):
        self.server.shutdown(); self.thread.join(); self.server.server_close()
        self.temporary.cleanup()
    def test_single_response_extraction_keeps_source_fields(self):
        sample=Path('fixtures/response.bin').read_bytes()
        rows=decode(sample,{'Content-Type':self.scenario['media']})
        self.assertEqual(rows,json.loads(Path('fixtures/current-rows.json').read_text(encoding='utf-8')))
    def test_real_http_to_saved_raw_and_document(self):
        result=collect_one(self.url,self.database)
        store=Store(self.database)
        try:
            self.assertEqual(result['count'],len(store.documents()))
            self.assertEqual(store.receipt(result['receipt'])['body'],Path('fixtures/response.bin').read_bytes())
            self.assertEqual(self.server.events[-1]['path'],self.scenario['entry_path'])
        finally:
            store.close()
    def test_repeated_current_response_does_not_duplicate_keys(self):
        first=collect_one(self.url,self.database)
        second=collect_one(self.url,self.database)
        self.assertEqual(first,second)
    def test_budget_stops_oversized_response(self):
        with self.assertRaisesRegex(ValueError,'max_bytes'):
            fetch(self.url,max_bytes=2)
    def test_error_response_retains_http_status(self):
        response=fetch(self.url.split(self.scenario['entry_path'])[0]+'/unknown')
        self.assertEqual(response.status,404)
    def test_store_duplicate_input_rejected_before_write(self):
        store=Store(self.database)
        response=fetch(self.url)
        try:
            with self.assertRaises(ValueError):
                store.save_response('r',response,[dict(key='x',value={}),dict(key='x',value={})])
            self.assertEqual(store.documents(),[])
        finally:
            store.close()

if __name__=='__main__':
    unittest.main()
