import unittest
import project

class TestProject(unittest.TestCase):
    def test_project_version(self):
        self.assertEqual(project.__version__, '0.0.1')