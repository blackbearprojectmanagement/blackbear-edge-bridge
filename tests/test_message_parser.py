from __future__ import annotations

import unittest

from app.message_parser import PLCMessageParseError, parse_plc_message


class TestParsePLCMessage(unittest.TestCase):
    def test_valid_mn(self) -> None:
        parsed = parse_plc_message('{"MN":"106-020C012P001 3241T01"}')

        self.assertEqual(parsed.message_type, "MN")
        self.assertEqual(parsed.part_data, "106-020C012P001")
        self.assertEqual(parsed.serial, "3241")
        self.assertEqual(parsed.table, "T01")

    def test_valid_mp(self) -> None:
        parsed = parse_plc_message('{"MP":"Z106-015C020P001 7084T02"}')

        self.assertEqual(parsed.message_type, "MP")
        self.assertEqual(parsed.part_data, "Z106-015C020P001")
        self.assertEqual(parsed.serial, "7084")
        self.assertEqual(parsed.table, "T02")

    def test_invalid_json(self) -> None:
        with self.assertRaisesRegex(PLCMessageParseError, "Malformed JSON"):
            parse_plc_message('{"MN":"106-020C012P001 3241T01"')

    def test_unsupported_key(self) -> None:
        with self.assertRaisesRegex(PLCMessageParseError, "Unsupported PLC message type"):
            parse_plc_message('{"XX":"106-020C012P001 3241T01"}')

    def test_missing_table_suffix(self) -> None:
        with self.assertRaisesRegex(PLCMessageParseError, "table suffix"):
            parse_plc_message('{"MN":"106-020C012P001 3241"}')

    def test_non_string_value(self) -> None:
        with self.assertRaisesRegex(PLCMessageParseError, "must be a string"):
            parse_plc_message('{"MN":3241}')


if __name__ == "__main__":
    unittest.main()
