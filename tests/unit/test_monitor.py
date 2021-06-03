import unittest
import unittest.mock as mock
import curses
from remake.monitor import RemakeMonitorCurses


class TestRemakeMonitorCurses(unittest.TestCase):
    @mock.patch('remake.monitor.sha1sum')
    @mock.patch('curses.curs_set')
    @mock.patch('curses.color_pair')
    @mock.patch('curses.init_pair')
    def test_class(self,
                   mock_init_pair, mock_color_pair,
                   mock_curs_set, mock_sha1sum):
        # Is there any value to this test?
        mock_sha1sum.return_value = 'mock_return'

        stdscr = mock.MagicMock()
        remake = mock.MagicMock()
        stdscr.getmaxyx.return_value = (100, 50)
        remake.name = 'mock'

        mock_color_pair.return_value = 0

        _mon = RemakeMonitorCurses(stdscr, remake, 1)
        _mon.cp('ERROR')
        _mon.ui('command', 'show', [], 'tasks', remake.name)
        status_counts = {
            'CANNOT_RUN': 10,
            'UNKNOWN': 11,
            'REMAINING': 4,
            'PENDING': 6,
            'RUNNING': 10,
            'COMPLETED': 1,
            'ERROR': 42,
        }
        _mon.summary(status_counts)
        _mon._check_i_offset(-5, 10)


if __name__ == '__main__':
    unittest.main()
