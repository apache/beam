import unittest
from ddt import ddt, data
import sync


@ddt
class FindMentionedTestCase(unittest.TestCase):

  @data(("sample text with mention @mention", ["mention"]),
        ("Data without mention", []),
        ("sample text with several mentions @first, @second @third", ["first", "second", "third"]))
  def test_findMentioned_finds_mentions_by_pattern(self, params):
    input, expectedResult = params
    result = sync.findMentioned(input)
    self.assertEqual(expectedResult, result)

  def test_findCommentReviewers(self):
    result = "some tesxt \n body"

if __name__ == '__main__':
  unittest.main()
