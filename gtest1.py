class TestPasswordHelper(TestCase):

    def setUp(self):
        PasswordHelper.password  = 'gt8est_12345678'
        PasswordHelper.hash_bytes = 64
        PasswordHelper.iterations = 10000
        PasswordHelper.salt_bytes = 64

        PasswordHelper._ALGORITHM_INDEX = 0
        PasswordHelper._ITERATION_INDEX = 1
        PasswordHelper._SALT_INDEX = 2
        PasswordHelper._HASH_INDEX = 3
        PasswordHelper._PART_COUNT = 4
        PasswordHelper._DELIMITER  = ';'
