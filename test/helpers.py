def safe_mock_s3(func):
    """
    Annotations will be evaluated even if `pytes -m "not s3"` is specified. So
      ```
      @pytest.mark.s3
      class TestS3(unittest.TestCase):
          @mock_s3
          def test_foo():
      ```
    will raise an error if moto is not installed.
    This decorator is used to avoid this error.
    """

    def wrapper(*args, **kwargs):
        from moto import mock_s3
        with mock_s3():
            return func(*args, **kwargs)

    return wrapper
