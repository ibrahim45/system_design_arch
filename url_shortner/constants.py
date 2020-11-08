TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def result_update(status=False, errors={}, message=""):
    result = {
        'status': status,
        'errors': errors,
        'message': message,
    }
    return result