from .runtime_type_check.runtime_type_check_test import BaseRunTimeTypeCheckTest


class RunTimeTypeCheckOffTest(BaseRunTimeTypeCheckTest):
    def __init__(self):
        self.runtime_type_check = False
        super(RunTimeTypeCheckOffTest, self).__init__()
