import unittest
from app import app, db

class AggregatorServiceIntegrationTest(unittest.TestCase):
    def setUp(self):
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        app.config['TESTING'] = True
        self.client = app.test_client()
        with app.app_context():
            db.create_all()

    def tearDown(self):
        with app.app_context():
            db.session.remove()
            db.drop_all()

    def test_full_crud(self):
        # Create
        res = self.client.post('/reports', json={'report_id': 'R100', 'content': 'Report Content'})
        self.assertEqual(res.status_code, 201)

        # Read
        res = self.client.get('/reports/R100')
        data = res.get_json()
        self.assertEqual(data['content'], 'Report Content')

        # Update
        res = self.client.put('/reports/R100', json={'content': 'Updated Content'})
        data = res.get_json()
        self.assertEqual(data['content'], 'Updated Content')

        # Delete
        res = self.client.delete('/reports/R100')
        self.assertEqual(res.status_code, 200)

if __name__ == '__main__':
    unittest.main()
