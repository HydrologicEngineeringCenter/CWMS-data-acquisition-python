import unittest

from CdaTextFile import CdaTextFile


class TimeSeriesFileTest(unittest.TestCase):

    def setUp(self):
        ctf = CdaTextFile("CDAInput.csv")
        self.series_list = ctf.get_time_series_list()

    def test_number_of_series(self):
        self.assertEqual(19, len(self.series_list))

    def test_thurmond_energy(self):
        thurmond_energy = "Thurmond.Energy-Sched.Total.1Hour.1Hour.Raw-test-data"
        x = [s for s in self.series_list if s["name"] == thurmond_energy]
        self.assertEqual (1, len(x),"should be just one series")
        series = x[0]
        self.assertEqual("SAS",series['office-id'])
        self.assertEqual("MWH",series['units'])
        values = series['values']
        self.assertEqual(24,len(values))
        bad_list = [v for v in values if abs(float(v[1])-102.00) > 0.01]
        self.assertEqual(0,len(bad_list))


if __name__ == '__main__':
    unittest.main()
