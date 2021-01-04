from RLTest import Env


def test_multilabel_filter():
    env = Env()
    with env.getConnection() as r:
        assert r.execute_command('TS.ADD s1 1 100 LABELS metric_family cpu metric_name user')
        assert r.execute_command('TS.ADD s2 2 55 LABELS metric_family cpu metric_name user')
        assert r.execute_command('TS.ADD s3 2 40 LABELS metric_family cpu metric_name system')
        assert r.execute_command('TS.ADD s1 2 95')

        actual_result = r.execute_command(
            'TS.mrange - + WITHLABELS FILTER metric_family=cpu GROUPBY metric_name REDUCE max')
        serie1 = actual_result[0]
        serie1_name = serie1[0]
        serie1_labels = serie1[1]
        serie1_values = serie1[2]
        env.assertEqual(serie1_values, [[2, b'40']])
        env.assertEqual(serie1_name, b'metric_name=system')
        env.assertEqual(serie1_labels[0][0], b'metric_name')
        env.assertEqual(serie1_labels[0][1], b'system')
        serie2 = actual_result[1]
        serie2_name = serie2[0]
        serie2_labels = serie2[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_name, b'metric_name=user')
        env.assertEqual(serie2_labels[0][0], b'metric_name')
        env.assertEqual(serie2_labels[0][1], b'user')
        env.assertEqual(serie2_values, [[1, b'100'], [2, b'95']])

        actual_result = r.execute_command(
            'TS.mrange - + WITHLABELS FILTER metric_family=cpu GROUPBY metric_name REDUCE sum')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, b'100'], [2, b'150']])

        actual_result = r.execute_command(
            'TS.mrange - + WITHLABELS FILTER metric_family=cpu GROUPBY metric_name REDUCE min')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, b'100'], [2, b'55']])
