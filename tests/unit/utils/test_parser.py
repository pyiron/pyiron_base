import os
from unittest import TestCase
from pyiron_base.utils.parser import Logstatus


class TestParser(TestCase):
    def setUp(self):
        self.file_name = "test_parser.txt"

    def tearDown(self):
        if os.path.exists(self.file_name):
            os.remove(self.file_name)

    def test_log_status_parser(self):
        input_str = """\
        alat    3.2     # lattice constant (would be in a more realistic example in the structure file)
        alpha   0.1     # noise amplitude
        a_0     3       # equilibrium lattice constant
        a_1     0
        a_2     1.0     # 2nd order in energy (corresponds to bulk modulus)
        a_3     0.0     # 3rd order
        a_4     0.0     # 4th order
        count   10      # number of calls (dummy)
        epsilon 0.2     # energy prefactor of lennard jones
        sigma   2.4     # distance unit of lennard jones
        cutoff  4.0     # cutoff length (relative to sigma)
        write_restart True
        read_restart False
        """
        with open(self.file_name, "w") as f:
            f.writelines(input_str)

        tag_dict = {
            "alat": {"arg": "0", "rows": 0},
            "count": {"arg": "0", "rows": 0},
            "energy": {"arg": "0", "rows": 0},
        }
        lf = Logstatus()
        lf.extract_file(file_name=self.file_name, tag_dict=tag_dict)
        self.assertEqual(lf.status_dict["alat"], [[[0], 3.2]])
        self.assertEqual(lf.status_dict["count"], [[[0], 10]])
