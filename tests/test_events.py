import importlib.util
from pathlib import Path
import unittest

p = Path(__file__).resolve().parents[1] / "producer" / "generate_events.py"
spec = importlib.util.spec_from_file_location("producer_module", p)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class EventTests(unittest.TestCase):
    def test_deterministic(self):
        self.assertEqual(module.event_for(3), module.event_for(3))

    def test_positive_quantity(self):
        self.assertGreater(module.event_for(42)["quantity"], 0)

    def test_invalid_index(self):
        with self.assertRaises(ValueError):
            module.event_for(0)

    def test_adversarial_cases(self):
        records = module.adversarial_events()
        self.assertEqual(len(records), 6)
        self.assertEqual(records[0], module.event_for(1))
        self.assertTrue(any(isinstance(r, dict) and r.get("quantity", 0) < 0 for r in records))
        self.assertTrue(any(isinstance(r, dict) and r.get("event_id") is None for r in records))
        self.assertTrue(any(isinstance(r, str) for r in records))
        self.assertTrue(any(isinstance(r, dict) and not str(r.get("event_id", "")).strip() for r in records))
        self.assertTrue(any(isinstance(r, dict) and r.get("unit_price") == float("inf") for r in records))


if __name__ == "__main__":
    unittest.main()
