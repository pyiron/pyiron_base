import unittest
from pyiron_base.jobs.datamining import _from_pickle

dill_str_1 = 'gASV9AIAAAAAAAB9lCiMCmdldF9qb2JfaWSUiIwJZ2V0X2luY2FylImMCWdldF9zaWdtYZSJjBln\nZXRfdG90YWxfbnVtYmVyX29mX2F0b21zlImMDGdldF9lbGVtZW50c5SJjBVnZXRfY29udmVyZ2Vu\nY2VfY2hlY2uUiYwVZ2V0X251bWJlcl9vZl9zcGVjaWVzlImMGWdldF9udW1iZXJfb2ZfaW9uaWNf\nc3RlcHOUiYwKZ2V0X2lzbWVhcpSJjAlnZXRfZW5jdXSUiYwKZ2V0X25fa3B0c5SJjA5nZXRfbl9l\ncXVfa3B0c5SJjCRnZXRfbnVtYmVyX29mX2ZpbmFsX2VsZWN0cm9uaWNfc3RlcHOUiYwUZ2V0X21h\nam9yaXR5X3NwZWNpZXOUiYwMZ2V0X2pvYl9uYW1llImMDmdldF9lbmVyZ3lfdG90lImMDmdldF9l\nbmVyZ3lfcG90lImMD2dldF9lbmVyZ3lfZnJlZZSJjA5nZXRfZW5lcmd5X2ludJSJjBdnZXRfZW5l\ncmd5X3RvdF9wZXJfYXRvbZSJjBdnZXRfZW5lcmd5X3BvdF9wZXJfYXRvbZSJjBhnZXRfZW5lcmd5\nX2ZyZWVfcGVyX2F0b22UiYwXZ2V0X2VuZXJneV9pbnRfcGVyX2F0b22UiYwQZ2V0X2VfY29udl9s\nZXZlbJSJjAxnZXRfZl9zdGF0ZXOUiYwKZ2V0X2VfYmFuZJSJjB5nZXRfbWFqb3JpdHlfY3J5c3Rh\nbF9zdHJ1Y3R1cmWUiYwaZ2V0X2VxdWlsaWJyaXVtX3BhcmFtZXRlcnOUiYwNZ2V0X3N0cnVjdHVy\nZZSJjApnZXRfZm9yY2VzlImMFmdldF9tYWduZXRpY19zdHJ1Y3R1cmWUiYwRZ2V0X2F2ZXJhZ2Vf\nd2F2ZXOUiYwPZ2V0X3BsYW5lX3dhdmVzlImMDmdldF9la2luX2Vycm9ylImMCmdldF92b2x1bWWU\niYwTZ2V0X3ZvbHVtZV9wZXJfYXRvbZSJdS4=\n'
dill_str_2 = 'gASVIQUAAAAAAAB9lCiMBGFfZXGUjApkaWxsLl9kaWxslIwQX2NyZWF0ZV9mdW5jdGlvbpSTlCho\nAowMX2NyZWF0ZV9jb2RllJOUKEMAlEsBSwBLAEsBSwNLE0MMiAGIAHwAgwFpAVMAlE6FlCmMA2pv\nYpSFlIxCL1VzZXJzL2phbnNzZW4vcHJvamVjdHMvcHlpcm9uX2Jhc2UvcHlpcm9uX2Jhc2Uvam9i\ncy9kYXRhbWluaW5nLnB5lIwIPGxhbWJkYT6US1lDAgwAlIwEaXRlbZSMA2tleZSGlCl0lFKUY3B5\naXJvbl9iYXNlLmpvYnMuZGF0YW1pbmluZwpfX2RpY3RfXwpoDU5oAowMX2NyZWF0ZV9jZWxslJOU\nToWUUpRoFU6FlFKUhpR0lFKUfZR9lCiMD19fYW5ub3RhdGlvbnNfX5R9lIwMX19xdWFsbmFtZV9f\nlIwvRnVuY3Rpb25Db250YWluZXIuX19zZXRpdGVtX18uPGxvY2Fscz4uPGxhbWJkYT6UdYaUYowI\nYnVpbHRpbnOUjAdnZXRhdHRylJOUjARkaWxslIwFX2RpbGyUk5SMCF9zZXRhdHRylGgkjAdzZXRh\ndHRylJOUh5RSlGgZjA1jZWxsX2NvbnRlbnRzlGgBh5RSMGguaBdoL2gEKGgGKEMCAAGUSwFLAEsA\nSwFLAktDQwx8AGQBGQBkAhMAUwCUTowZb3V0cHV0L2VxdWlsaWJyaXVtX3ZvbHVtZZRHP9VVVVVV\nVVWHlCloCoWUjE4vdmFyL2ZvbGRlcnMvOXAvcnp0eXYwNmQweHY0aDI2Y3l2OG5ydzNtMDAwMGdx\nL1QvaXB5a2VybmVsXzUwODA2LzE1NTkyNTk0MzEucHmUjBVnZXRfbGF0dGljZV9wYXJhbWV0ZXKU\nSwJDAgwBlCkpdJRSlGNfX2J1aWx0aW5fXwpfX21haW5fXwpoN05OdJRSlH2UfZRoH32Uc4aUYoeU\nUjCMDGJ1bGtfbW9kdWx1c5RoBChoE2NweWlyb25fYmFzZS5qb2JzLmRhdGFtaW5pbmcKX19kaWN0\nX18KaA1OaBVOhZRSlGgVToWUUpSGlHSUUpR9lH2UKGgffZRoIWgidYaUYmguaEZoL2hCh5RSMGgu\naERoL2gEKGgGKEMCAAGUSwFLAEsASwFLAktDQwh8AGQBGQBTAJROjB9vdXRwdXQvZXF1aWxpYnJp\ndW1fYnVsa19tb2R1bHVzlIaUKWgKhZRoNowGZ2V0X2JtlEsGQwIIAZQpKXSUUpRjX19idWlsdGlu\nX18KX19tYWluX18KaFROTnSUUpR9lH2UaB99lHOGlGKHlFIwjAlwb3RlbnRpYWyUaAQoaBNjcHlp\ncm9uX2Jhc2Uuam9icy5kYXRhbWluaW5nCl9fZGljdF9fCmgNTmgVToWUUpRoFU6FlFKUhpR0lFKU\nfZR9lChoH32UaCFoInWGlGJoLmhjaC9oX4eUUjBoLmhhaC9oBChoBihDBAABFAGUSwFLAEsASwJL\nBEtDQxx8AGoAoAF8AGQBGQBkAhkAoQF9AXwBZAMZAFMAlChOjAlvdXRwdXQvaWSUSwCMFGlucHV0\nL3BvdGVudGlhbC9OYW1llHSUjAdwcm9qZWN0lIwHaW5zcGVjdJSGlGgKjAVjaGlsZJSGlGg2jAdn\nZXRfcG90lEsKQwQUAQgBlCkpdJRSlGNfX2J1aWx0aW5fXwpfX21haW5fXwpodk5OdJRSlH2UfZRo\nH32Uc4aUYoeUUjB1Lg==\n'


try:
    import dill

    skip_dill_test = False
except ImportError:
    skip_dill_test = True


class TestDillCloudpickleComptaibility(unittest.TestCase):
    def test_1(self):
        output = _from_pickle(hdf={"test": dill_str_1}, key="test")
        self.assertEqual(len(output.keys()), 36)
        self.assertTrue(output['get_job_id'])
        for k in output.keys():
            if k != 'get_job_id':
                self.assertFalse(output[k])

    @unittest.skipIf(
        skip_dill_test, "dill is not installed, so the dill tests are skipped."
    )
    def test_2(self):
        output = _from_pickle(hdf={"test": dill_str_2}, key="test")
        self.assertEqual(len(output.keys()), 3)
        self.assertEqual(list(output.keys()), ['a_eq', 'bulk_modulus', 'potential'])
        for k in output.keys():
            self.assertTrue(callable(output[k]))
