# noinspection PyArgumentList,PyUnresolvedReferences,PyTypeChecker
class EnumToChoicesMixin:
    @classmethod
    def choices(cls):
        return tuple((enum.value, "{}".format(cls(enum.value).name)) for enum in cls)
