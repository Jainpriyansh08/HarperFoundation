from uuid import UUID


def is_valid_uuid(uuid_to_test, version=4):
        uuid_to_test = uuid_to_test.strip()
        try:
            uuid_obj = UUID(uuid_to_test, version=version)
        except ValueError:
            return False
        return True