import importlib.util
from pathlib import Path
import unittest

p = Path(__file__).resolve().parents[1] / "producer" / "generate_events.py"
spec = importlib.util.spec_from_file_location("producer_module", p)
module = importlib.util.module_from_spec(spec)
# Importing the producer requires kafka-python (see requirements.txt).
spec.loader.exec_module(module)


class EventTests(unittest.TestCase):
    def test_deterministic(self):
        self.assertEqual(module.event_for(3), module.event_for(3))

    def test_positive_quantity(self):
        self.assertGreater(module.event_for(42)["quantity"], 0)

    def test_invalid_index(self):
        with self.assertRaises(ValueError):
            module.event_for(0)


if __name__ == "__main__":
    unittest.main()
