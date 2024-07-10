import unittest

from pyiron_base.utils.strtobool import strtobool


class TestStrtobool(unittest.TestCase):
    def test_letters(self):
        for val in ("y", "yes", "t", "true", "on", "1"):
            self.assertTrue(strtobool(val))
        for val in ("Y", "YEs", "T", "tRue", "oN", "1"):
            self.assertTrue(strtobool(val))
        for val in ("n", "no", "f", "false", "off", "0"):
            self.assertFalse(strtobool(val))
        with self.assertRaises(ValueError):
            strtobool("Vegetarians do not eat Bratwurst")


if __name__ == "__main__":
    unittest.main()
