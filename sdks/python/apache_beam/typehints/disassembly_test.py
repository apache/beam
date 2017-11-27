import unittest

from apache_beam.typehints.disassembly import Instruction, get_instructions

# Uncomment the line below to compare with the Python 3.4 dis library
# from dis import Instruction, get_instructions


class InstructionsTest(unittest.TestCase):
  def testFunction(self):
    actual = list(get_instructions(lambda x: [x, 10 - x]))
    expected = [Instruction(opname='LOAD_FAST', opcode=124, arg=0, argval='x',
                            argrepr='x', offset=0, starts_line=11,
                            is_jump_target=False),
                Instruction(opname='LOAD_CONST', opcode=100, arg=1, argval=10,
                            argrepr='10', offset=3, starts_line=None,
                            is_jump_target=False),
                Instruction(opname='LOAD_FAST', opcode=124, arg=0, argval='x',
                            argrepr='x', offset=6, starts_line=None,
                            is_jump_target=False),
                Instruction(opname='BINARY_SUBTRACT', opcode=24, arg=None,
                            argval=None, argrepr='', offset=9, starts_line=None,
                            is_jump_target=False),
                Instruction(opname='BUILD_LIST', opcode=103, arg=2, argval=2,
                            argrepr='', offset=10, starts_line=None,
                            is_jump_target=False),
                Instruction(opname='RETURN_VALUE', opcode=83, arg=None,
                            argval=None, argrepr='', offset=13,
                            starts_line=None, is_jump_target=False)]
    self.assertEqual(actual, expected)

  def testLambda(self):
    def f(x):
      z = 2
      return z - x

    actual = list(get_instructions(f))
    expected = [
        Instruction(opname='LOAD_CONST', opcode=100, arg=1, argval=2,
                    argrepr='2', offset=0, starts_line=34,
                    is_jump_target=False),
        Instruction(opname='STORE_FAST', opcode=125, arg=1, argval='z',
                    argrepr='z', offset=3, starts_line=None,
                    is_jump_target=False),
        Instruction(opname='LOAD_FAST', opcode=124, arg=1, argval='z',
                    argrepr='z', offset=6, starts_line=35,
                    is_jump_target=False),
        Instruction(opname='LOAD_FAST', opcode=124, arg=0, argval='x',
                    argrepr='x', offset=9, starts_line=None,
                    is_jump_target=False),
        Instruction(opname='BINARY_SUBTRACT', opcode=24, arg=None, argval=None,
                    argrepr='', offset=12, starts_line=None,
                    is_jump_target=False),
        Instruction(opname='RETURN_VALUE', opcode=83, arg=None, argval=None,
                    argrepr='', offset=13, starts_line=None,
                    is_jump_target=False)]
    self.assertEqual(actual, expected)

  def testBoundMethod(self):
    class A(object):
      def m(self, x):
        return x + 15.3

    actual = list(get_instructions(A().m))
    expected = [Instruction(opname='LOAD_FAST', opcode=124, arg=1, argval='x',
                            argrepr='x', offset=0, starts_line=62,
                            is_jump_target=False),
                Instruction(opname='LOAD_CONST', opcode=100, arg=1, argval=15.3,
                            argrepr='15.3', offset=3, starts_line=None,
                            is_jump_target=False),
                Instruction(opname='BINARY_ADD', opcode=23, arg=None,
                            argval=None, argrepr='', offset=6, starts_line=None,
                            is_jump_target=False),
                Instruction(opname='RETURN_VALUE', opcode=83, arg=None,
                            argval=None, argrepr='', offset=7, starts_line=None,
                            is_jump_target=False)]
    self.assertEqual(actual, expected)
